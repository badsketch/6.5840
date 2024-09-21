package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type CoordinatorState int

const (
	MAP CoordinatorState = iota
	REDUCE
)

type WorkerState int

const (
	IN_PROGRESS WorkerState = iota
	IDLE
)

type WorkerStatus struct {
	State WorkerState
	Files []string
}

type Coordinator struct {
	FileQueue   [][]string
	mu          sync.Mutex
	WorkerPool  map[int]WorkerStatus
	buckets     int
	State       CoordinatorState
	StateStream chan struct{}
}

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.mu.Lock()
	id := len(c.WorkerPool) + 1
	c.mu.Unlock()
	reply.ID = id
	reply.BucketCount = c.buckets
	fmt.Printf("A worker has joined. Given ID %v\n", id)
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
			State: IN_PROGRESS,
			Files: files,
		}
		reply.Files = files
		// otherwise return nothing in reply
	} else {
		c.WorkerPool[args.ID] = WorkerStatus{
			State: IDLE,
			Files: []string{},
		}
		reply.Files = []string{}
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
	if c.IsMappingDone() {
		c.StateStream <- struct{}{}
	}
	c.mu.Unlock()
	return nil
}

// dangerous. mutex should be handled in caller
func (c *Coordinator) IsMappingDone() bool {
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	// awkward, but this allows map and reduce to use the same WorkerStatus
	// field for files. Though map is simple a list of a single file
	filesList := [][]string{}
	for _, f := range files {
		filesList = append(filesList, []string{f})
	}

	c := Coordinator{
		FileQueue:   filesList,
		WorkerPool:  make(map[int]WorkerStatus),
		buckets:     nReduce,
		State:       MAP,
		StateStream: make(chan struct{}),
	}
	c.server()

	// Listen for when we're ready to enter REDUCE phase
	go func() {
		<-c.StateStream
		c.State = REDUCE
		fmt.Println("MAP Phase completed. Moving to REDUCE")
	}()

	return &c
}
