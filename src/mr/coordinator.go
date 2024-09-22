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
	State WorkerState
	Files []string
}

type Coordinator struct {
	FileQueue   [][]string
	mu          sync.Mutex
	WorkerPool  map[int]WorkerStatus
	buckets     int
	State       CoordinatorPhase
	StateStream chan struct{}
}

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	c.mu.Lock()
	id := len(c.WorkerPool) + 1
	c.WorkerPool[id] = WorkerStatus{
		State: IDLE,
		Files: []string{},
	}
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
	return c.State == DONE
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
		for {
			<-c.StateStream
			fmt.Println("asdfasdfasdf")
			if c.State == MAP {
				fmt.Println("MAP Phase completed. Moving to REDUCE")
				c.convertBucketsToReduceTasks()
				fmt.Printf("REDUCE queue is now %v\n", c.FileQueue)
				c.State = REDUCE
			} else if c.State == REDUCE {
				fmt.Println("REDUCE Phase completed. Moving to DONE and shutting down...")
				c.State = DONE
			} else {
				fmt.Println("what happened")
			}
		}
	}()

	return &c
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
	c.mu.Lock()
	c.FileQueue = partitions
	c.mu.Unlock()
}
