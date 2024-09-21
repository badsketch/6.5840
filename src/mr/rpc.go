package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type RegisterWorkerArgs struct{}
type RegisterWorkerReply struct {
	ID int
}

type GetWorkArgs struct {
	ID int
}
type GetWorkReply struct {
	File string
}

type SignalWorkDoneArgs struct {
	ID int
}
type SignalWorkDoneReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
