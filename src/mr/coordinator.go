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

type WorkerState int

const (
	IN_PROGRESS WorkerState = iota
	IDLE
)

type WorkerStatus struct {
	State WorkerState
	File  string
}

type Coordinator struct {
	FileQueue  []string
	mu         sync.Mutex
	WorkerPool map[int]WorkerStatus
}

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.mu.Lock()
	id := len(c.WorkerPool) + 1
	c.mu.Unlock()
	reply.ID = id
	fmt.Printf("A worker has joined. Given ID %v\n", id)
	return nil
}

func (c *Coordinator) GetWork(args *GetWorkArgs, reply *GetWorkReply) error {
	c.mu.Lock()
	// pop from front of queue if there's tasks
	if len(c.FileQueue) > 0 {
		file := c.FileQueue[0]
		c.FileQueue = c.FileQueue[1:]
		// update worker states
		c.WorkerPool[args.ID] = WorkerStatus{
			State: IN_PROGRESS,
			File:  file,
		}
		reply.File = file
		// otherwise return nothing in reply
	} else {
		c.WorkerPool[args.ID] = WorkerStatus{
			State: IDLE,
			File:  "",
		}
		reply.File = ""
	}
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) SignalWorkDone(args *SignalWorkDoneArgs, reply *SignalWorkDoneReply) error {
	c.mu.Lock()
	c.WorkerPool[args.ID] = WorkerStatus{
		State: IDLE,
		File:  "",
	}
	c.mu.Unlock()
	return nil
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
	c := Coordinator{
		FileQueue:  files,
		WorkerPool: make(map[int]WorkerStatus),
	}

	// Your code here.

	c.server()
	return &c
}
