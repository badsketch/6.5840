package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// register yourself
	id, numBuckets := CallRegister()
	for {
		files, action := CallGetWork(id)
		if action == MAP {
			ProcessMapTask(files, id, numBuckets, mapf)
			CallSignalWorkDone(id)
		} else if action == REDUCE {
			ProcessReduceTask(files, reducef)
			CallSignalWorkDone(id)
		} else {
			fmt.Println("Did not receive work. Sleeping...")
			time.Sleep(4 * time.Second)
		}
	}

}

func CallRegister() (int, int) {
	args := RegisterWorkerArgs{}
	reply := RegisterWorkerReply{}
	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if ok {
		fmt.Printf("Joined as Worker #%v\n", reply.ID)
		return reply.ID, reply.BucketCount
	} else {
		panic("Error when worker attempting to register!")
	}
}

func CallGetWork(id int) ([]string, WorkerPhase) {
	// request work
	args := GetWorkArgs{
		ID: id,
	}
	reply := GetWorkReply{}
	ok := call("Coordinator.GetWork", &args, &reply)
	if ok {
		if len(reply.Files) > 0 {
			fmt.Printf("Worker %v is now running %v on %v\n", id, reply.Action, reply.Files)
		} else {
			fmt.Printf("Worker %v did not receive any work.\n", id)
		}
		return reply.Files, reply.Action
	} else {
		panic("Error when worker attempting to request work!")
	}
}

func CallSignalWorkDone(id int) {
	args := SignalWorkDoneArgs{
		ID: id,
	}
	reply := SignalWorkDoneReply{}
	ok := call("Coordinator.SignalWorkDone", &args, &reply)
	if ok {
		fmt.Printf("Worker %v signaled that work was completed\n", id)
	} else {
		panic("Error when worker signaling work done!")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

func ProcessMapTask(files []string, id int, numBuckets int, mapf func(string, string) []KeyValue) {
	kva := applyMapToFiles(files, mapf)
	partitionKVToBuckets(id, numBuckets, kva)
}

func applyMapToFiles(filenames []string, mapf func(string, string) []KeyValue) []KeyValue {
	kva := []KeyValue{}
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("Cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva = append(kva, mapf(filename, string(content))...)
	}
	return kva
}

// TODO: maybe pass pointer to kv for efficiency
// probably very inefficient with all the file opening and closing. Also consider sorting
// KV format may need to be changed since it has KEY VALUE literal strings in the result,
// but this could work as long as reducing expects this
func partitionKVToBuckets(id int, numBuckets int, kva []KeyValue) {
	for _, KV := range kva {
		bucket := ihash(KV.Key) % numBuckets
		destFile := fmt.Sprintf("mr-%v-%v", id, bucket)
		// TODO: create this at the beginning of the application
		// currently manually created
		fullPath := filepath.Join("mr-intermediate", destFile)
		file, err := os.OpenFile(fullPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			panic(fmt.Sprintf("Error when trying to create intermediate files:%v", err))
		}
		enc := json.NewEncoder(file)
		// err = enc.Encode(map[string]string{KV.Key: KV.Value})
		err = enc.Encode(KV)
		if err != nil {
			panic(fmt.Sprintf("Error when trying to encode to %v: %v", destFile, err))
		}
		file.Close()
	}
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func ProcessReduceTask(filenames []string, reducef func(string, []string) string) {
	// TODO: figure out a better way to determine the reduce task name
	first := filenames[0]
	parts := strings.Split(first, "-")
	reduceTaskNo, err := strconv.Atoi(parts[len(parts)-1])
	if err != nil {
		panic(fmt.Sprintf("Error when parsing file to get reduce file name: %v", first))
	}
	fmt.Println("Doing some reduce stuff!")
	// decode all of the kv and append to giant map
	kva := []KeyValue{}
	for _, filename := range filenames {
		file, err := os.Open(filepath.Join("mr-intermediate", filename))
		if err != nil {
			panic(fmt.Sprintf("Error while attempting to open %v: %v", file, err))
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	// sort on the map
	sort.Sort(ByKey(kva))

	// perform reduce
	oname := fmt.Sprintf("mr-out-%v", reduceTaskNo)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
}
