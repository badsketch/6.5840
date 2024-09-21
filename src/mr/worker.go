package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
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
	id := CallRegister()
	for {
		file := CallGetWork(id)
		if len(file) > 0 {
			applyMapToFile(file, mapf)
			CallSignalWorkDone(id)
		} else {
			fmt.Println("Did not receive work. Sleeping...")
			time.Sleep(4 * time.Second)
		}
	}

}

func CallRegister() int {
	args := RegisterWorkerArgs{}
	reply := RegisterWorkerReply{}
	ok := call("Coordinator.RegisterWorker", &args, &reply)
	if ok {
		fmt.Printf("Joined as Worker #%v\n", reply.ID)
		return reply.ID
	} else {
		panic("Error when worker attempting to register!")
	}
}

func CallGetWork(id int) string {
	// request work
	args := GetWorkArgs{
		ID: id,
	}
	reply := GetWorkReply{}
	ok := call("Coordinator.GetWork", &args, &reply)
	if ok {
		if len(reply.File) > 0 {
			fmt.Printf("Worker %v is now processing %v\n", id, reply.File)
		} else {
			fmt.Printf("Worker %v did not receive any work.\n", id)
		}
		return reply.File
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

func applyMapToFile(filename string, mapf func(string, string) []KeyValue) []KeyValue {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	return kva
}
