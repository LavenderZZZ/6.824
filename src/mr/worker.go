package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func doMapTask(task *Task, mapf func(string, string) []KeyValue) {

	intermediate := []KeyValue{}
	filename := task.Filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	intermediateMap := make(map[int][]KeyValue, task.NReduce)
	for _, kv := range intermediate {
		reduceId := ihash(kv.Key) % task.NReduce
		intermediateMap[reduceId] = append(intermediateMap[reduceId], kv)
	}

	sort.Sort(ByKey(intermediate))

	for i := 0; i < task.NReduce; i++ {
		// write intermediate to file json format
		oname := fmt.Sprintf("mr-tmp-%d-%d", task.TaskId, i)
		ofile, _ := os.Create(oname)
		encoder := json.NewEncoder(ofile)
		for _, kv := range intermediateMap[i] {
			err := encoder.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot write %v", oname)
			}
		}
		ofile.Close()
	}

}

func doReduceTask(task *Task, reducef func(string, []string) string) {

	// read intermediate from file json format
	intermediate := []KeyValue{}
	for i := 0; i < task.NReduce; i++ {
		oname := fmt.Sprintf("mr-tmp-%d-%d", i, task.TaskId)
		file, err := os.Open(oname)
		if err != nil {
			log.Fatalf("cannot open %v", oname)
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	oname := fmt.Sprintf("mr-out-%d", task.TaskId)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()

}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	flag := false
	for {
		task := &Task{}
		if flag {
			task.TaskType = Waiting
			flag = false
		}
		response := Calltask(task)
		log.Printf("worker %d get response from heatbeat is %v", 1, response)
		switch task.TaskType {
		case Waiting:
			time.Sleep(time.Second)
			flag = true
		case Map:
			log.Printf("worker %d get map task %v", 1, response)
			doMapTask(task, mapf)

		case Reduce:
			log.Printf("worker %d get reduce task %v", 1, response)
			doReduceTask(task, reducef)
		case Finished:
			return
		default:
			panic("unknow task type")
		}

	}

}

func FinishTask(task *Task) bool {
	res := &ExampleReply{}
	ok := call("Coordinator.finishTask", task, res)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("task finished .Y %v\n", task.TaskId)
	} else {
		fmt.Printf("call failed!\n")
	}
	return ok
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func Calltask(task *Task) bool {

	// declare an argument structure.
	args := RequestTaskArgs{}
	if task.TaskType == Waiting {
		args.status = Waiting
	}
	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Heartbeat", &args, task)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", task.TaskType)
	} else {
		fmt.Printf("call failed!\n")
	}
	return ok
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
