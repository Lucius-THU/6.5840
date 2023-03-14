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

const MapTask, ReduceTask, WaitTask, FinishTask int = 0, 1, 2, 3
const NotArranged, Waiting, Finished int = 0, 1, 2

type TaskArgs struct{}

type TaskReply struct {
	TaskType   int
	MapFile    string
	TaskNumber int
	NMap       int
	NReduce    int
}

type FinishArgs struct {
	MapTaskNumber    int
	ReduceTaskNumber int
}

type FinishReply struct{}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
