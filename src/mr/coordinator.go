package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	State       int // 0: map, 1: reduce 2 done
	MapChan     chan *Task
	MapTasks    []*Task
	ReduceChan  chan *Task
	ReduceTasks []*Task
	RemainTasks int
	mutex       sync.Mutex
}

func (c *Coordinator) finishTask(task *Task) error {
	c.mutex.Lock()
	if task.TaskType == Map {
		c.MapTasks[task.TaskId].Finished = true
		c.RemainTasks--
	} else {
		c.ReduceTasks[task.TaskId].Finished = true
		c.RemainTasks--
	}
	c.mutex.Unlock()
	c.checkFinish(task.NReduce)
	return nil
}

func (c *Coordinator) checkFinish(Nreduce int) bool {
	if c.RemainTasks == 0 {
		if len(c.ReduceTasks) == 0 {
			c.createReduceTasks(Nreduce)
			return false
		}
		return true
	}
	return false
}

func (c *Coordinator) createReduceTasks(nReduce int) {
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks = append(c.ReduceTasks, &Task{
			TaskType: Reduce,
			NReduce:  nReduce,
			TaskId:   i,
			Finished: false,
			Start:    time.Now(),
		})
		c.ReduceChan <- c.ReduceTasks[i]
	}
	c.State = Reduce
	c.RemainTasks = nReduce
}

func (c *Coordinator) Heartbeat(args *RequestTaskArgs, reply *Task) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.State == Map {
		if len(c.MapChan) > 0 {
			*reply = *<-c.MapChan
			reply.Start = time.Now()
			return nil
		} else {
			if args.status != Waiting {
				reply.TaskType = Waiting
			} else {
				for i := 0; i < len(c.MapTasks); i++ {
					if !c.MapTasks[i].Finished && time.Now().Sub(c.MapTasks[i].Start).Seconds() > 10 {
						*reply = *c.MapTasks[i]
						c.MapTasks[i].Start = time.Now()
						return nil
					}
				}
			}
		}
	} else if c.State == Reduce {
		if len(c.ReduceChan) > 0 {
			*reply = *<-c.ReduceChan
			reply.Start = time.Now()
			return nil
		} else {
			if args.status != Waiting {
				reply.TaskType = Waiting
			} else {
				for i := 0; i < len(c.ReduceTasks); i++ {
					if !c.ReduceTasks[i].Finished && time.Now().Sub(c.ReduceTasks[i].Start).Seconds() > 10 {
						*reply = *c.ReduceTasks[i]
						c.ReduceTasks[i].Start = time.Now()
						return nil
					}
				}
			}
		}
	} else {
		reply.TaskType = Finished
	}
	return nil
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {

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
	// Your code here.
	c := Coordinator{
		State:       Map,
		MapChan:     make(chan *Task),
		MapTasks:    make([]*Task, len(files)),
		ReduceChan:  make(chan *Task),
		ReduceTasks: make([]*Task, 0),
		RemainTasks: len(files),
	}
	i := 0
	for _, file := range files {
		c.MapTasks[i] = &Task{
			TaskType: Map,
			Filename: file,
			NReduce:  nReduce,
			TaskId:   i,
			Finished: false,
			Start:    time.Now(),
		}
		c.MapChan <- c.MapTasks[i]
		i++
	}

	c.server()
	return &c
}
