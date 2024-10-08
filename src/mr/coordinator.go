package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type CoordinatorPhase int

const (
	MAP CoordinatorPhase = iota
	REDUCE
	WAIT
	DONE
)

type WorkerState int

const (
	IN_PROGRESS WorkerState = iota
	IDLE
)

type WorkerStatus struct {
	State    WorkerState
	Files    []string
	Assigned time.Time
}

type Coordinator struct {
	FileQueue    [][]string
	mu           sync.Mutex
	WorkerPool   map[int]WorkerStatus
	buckets      int
	State        CoordinatorPhase
	StateStream  chan struct{}
	NextWorkerID int
}

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.mu.Lock()
	c.NextWorkerID = c.NextWorkerID + 1
	id := c.NextWorkerID
	c.WorkerPool[id] = WorkerStatus{
		State: IDLE,
		Files: []string{},
	}
	c.mu.Unlock()
	reply.ID = id
	reply.BucketCount = c.buckets
	log.Printf("A worker has joined. Given ID %v\n", id)
	return nil
}

func (c *Coordinator) GetWork(args *GetWorkArgs, reply *GetWorkReply) error {
	c.mu.Lock()
	// pop from front of queue if there's tasks
	if len(c.FileQueue) > 0 {
		files := c.FileQueue[0]
		c.FileQueue = c.FileQueue[1:]
		// update worker states
		c.WorkerPool[args.ID] = WorkerStatus{
			State:    IN_PROGRESS,
			Files:    files,
			Assigned: time.Now(),
		}
		reply.Files = files
		reply.Action = c.State
		// otherwise return nothing in reply
	} else {
		c.WorkerPool[args.ID] = WorkerStatus{
			State: IDLE,
			Files: []string{},
		}
		reply.Files = []string{}
		reply.Action = WAIT
	}
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) SignalWorkDone(args *SignalWorkDoneArgs, reply *SignalWorkDoneReply) error {
	c.mu.Lock()
	c.WorkerPool[args.ID] = WorkerStatus{
		State: IDLE,
		Files: []string{},
	}
	log.Println(c.WorkerPool)
	if c.IsCurrentPhaseDone() {
		c.StateStream <- struct{}{}
	}
	c.mu.Unlock()
	return nil
}

// dangerous. mutex should be handled in caller
func (c *Coordinator) IsCurrentPhaseDone() bool {
	return c.IsTaskQEmpty() && c.IsAllWorkersIdle()
}

// dangerous. mutex should be handled in caller
func (c *Coordinator) IsTaskQEmpty() bool {
	return len(c.FileQueue) == 0
}

// dangerous. mutex should be handled in caller
func (c *Coordinator) IsAllWorkersIdle() bool {
	idle := true
	for _, status := range c.WorkerPool {
		if status.State == IN_PROGRESS {
			idle = false
		}
	}
	return idle
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.State == DONE
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	log.SetFlags(log.Ltime | log.Lshortfile)

	// set up directory for intermediate files
	err := os.Mkdir("./mr-intermediate", 0755)
	if err != nil {
		panic(fmt.Sprintf("Error when creating temp file directory! %v", err))
	}

	// awkward, but this allows map and reduce to use the same WorkerStatus
	// field for files. Though map is simple a list of a single file
	filesList := [][]string{}
	for _, f := range files {
		filesList = append(filesList, []string{f})
	}

	log.Printf("Mapping the files: %v\n", filesList)

	c := Coordinator{
		FileQueue:    filesList,
		WorkerPool:   make(map[int]WorkerStatus),
		NextWorkerID: 0,
		buckets:      nReduce,
		State:        MAP,
		StateStream:  make(chan struct{}),
	}
	c.server()

	go c.watchStateChange()
	go c.monitorWorkerStatus()

	return &c
}

// listens to state stream to see if it needs to change from mapping, reduce, or finished
func (c *Coordinator) watchStateChange() {
	for {
		<-c.StateStream
		if c.State == MAP {
			log.Println("MAP Phase completed. Moving to REDUCE")
			c.convertBucketsToReduceTasks()
			c.mu.Lock()
			c.State = REDUCE
			c.mu.Unlock()
		} else if c.State == REDUCE {
			log.Println("REDUCE Phase completed. Moving to DONE and cleaning up...")
			c.cleanUp()
			c.mu.Lock()
			c.State = DONE
			c.mu.Unlock()
			log.Println("Shutting down.")
		} else {
			panic("Unexpected state transition!")
		}
	}
}

// deletes some temporary files like directory where mapped intermediate files are
// as well as temp files created during reduce phase
func (c *Coordinator) cleanUp() {
	err := os.RemoveAll("./mr-intermediate")
	if err != nil {
		panic(fmt.Sprintf("Error during temp file directory cleanup! %v", err))
	}

	files, err := os.ReadDir(".")
	if err != nil {
		panic(fmt.Sprintf("Error when trying to read directory during tempfile cleanup! %v", err))
	}
	for _, file := range files {
		if strings.HasPrefix(file.Name(), "mr-tempfile-") {
			err = os.Remove(file.Name())
			if err != nil {
				panic(fmt.Sprintf("Error during cleanup of tempfile %v: %v", file.Name(), err))
			}
		}
	}
}

// deletes any workers based on arbitrary duration (10 sec.)
// by assuming they've crashed
func (c *Coordinator) monitorWorkerStatus() {
	tickStream := time.NewTicker(time.Second)
	for {
		<-tickStream.C
		c.mu.Lock()
		for id, worker := range c.WorkerPool {
			if worker.State == IN_PROGRESS && time.Since(worker.Assigned) >= time.Second*10 {
				log.Printf("Worker %v has been stuck on %v. Shutting down and adding workload back to queue\n", id, worker.Files)
				delete(c.WorkerPool, id)
				c.FileQueue = append(c.FileQueue, worker.Files)
			}
		}
		c.mu.Unlock()
	}
}

// lists all files in directory, but there could be a better way
// to do this where coordinator collects map worker files
func (c *Coordinator) convertBucketsToReduceTasks() {
	// TODO: turn intermediate directory to a variable to be passed around
	dir := "./mr-intermediate"
	files, err := os.ReadDir(dir)
	if err != nil {
		panic(fmt.Sprintf("Error reading directory: %v\n", err))
	}

	partitions := [][]string{}
	// create empty partitions to be appended to later on
	for i := 0; i < c.buckets; i++ {
		partitions = append(partitions, []string{})
	}
	mrPrefix := "mr-"
	for _, file := range files {
		if strings.HasPrefix(file.Name(), mrPrefix) {
			// mr-1-4
			parts := strings.Split(file.Name(), "-")
			partition, err := strconv.Atoi(parts[len(parts)-1])
			if err != nil {
				panic(fmt.Sprintf("Error when parsing file %v: %v", file.Name(), err))
			}
			partitions[partition] = append(partitions[partition], file.Name())
		}
	}
	// sometimes all hashes go to a single partition, resulting in empty lists of files
	// remove those (ex. [[] [mr-1-2, mr-2-2] []])
	filteredPartitions := [][]string{}
	for _, partition := range partitions {
		if len(partition) > 0 {
			filteredPartitions = append(filteredPartitions, partition)
		}
	}
	c.mu.Lock()
	c.FileQueue = filteredPartitions
	log.Printf("REDUCE queue is now %v\n", c.FileQueue)
	c.mu.Unlock()
}
