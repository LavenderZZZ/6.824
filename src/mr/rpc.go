package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

const Map = 1
const Reduce = 2
const Waiting = 3
const Finished = 4

type Task struct {
	TaskType int
	Filename string
	NReduce  int
	TaskId   int
	Finished bool
	Start    time.Time
}
type TaskRequest struct {
	X int
}

type TaskReply struct {
	XTask            Task
	NumMapTask       int
	NumReduceTask    int
	CurNumMapTask    int
	CurNumReduceTask int
}

// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type RequestTaskArgs struct {
	Status int
}

type RequestTaskReply struct {
	TaskType int
	TaskId   int
	FileName string
	NReduce  int
	NMap     int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
