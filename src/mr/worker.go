package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		args := TaskArgs{}
		reply := TaskReply{}
		ok := CallForTask(&args, &reply)
		if !ok || reply.TaskType == FinishTask {
			break
		}
		switch reply.TaskType {
		case MapTask:
			doMapTask(mapf, reply)
		case ReduceTask:
			doReduceTask(reducef, reply)
		default:
			time.Sleep(time.Second)
		}
	}
}

func doMapTask(mapf func(string, string) []KeyValue, reply TaskReply) {
	file, err := os.Open(reply.MapFile)
	if err != nil {
		log.Fatalf("cannot open %v", reply.MapFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.MapFile)
	}
	file.Close()
	intermediate := mapf(reply.MapFile, string(content))
	buckets := make([][]KeyValue, reply.NReduce)
	for _, kv := range intermediate {
		id := ihash(kv.Key) % reply.NReduce
		buckets[id] = append(buckets[id], kv)
	}
	for i := range buckets {
		filename := "mr-" + strconv.Itoa(reply.TaskNumber) + "-" + strconv.Itoa(i)
		tempFile, _ := ioutil.TempFile("", filename+"*")
		enc := json.NewEncoder(tempFile)
		for _, kv := range buckets[i] {
			enc.Encode(&kv)
		}
		os.Rename(tempFile.Name(), filename)
		tempFile.Close()
	}
	finishArgs := FinishArgs{reply.TaskNumber, -1}
	finishReply := FinishReply{}
	CallForFinish(&finishArgs, &finishReply)
}

func doReduceTask(reducef func(string, []string) string, reply TaskReply) {
	intermediate := []KeyValue{}
	for i := 0; i < reply.NMap; i++ {
		filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskNumber)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(reply.TaskNumber)
	tempFile, _ := ioutil.TempFile("", oname+"*")
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
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	os.Rename(tempFile.Name(), oname)
	tempFile.Close()
	finishArgs := FinishArgs{-1, reply.TaskNumber}
	finishReply := FinishReply{}
	CallForFinish(&finishArgs, &finishReply)
}

func CallForTask(args *TaskArgs, reply *TaskReply) bool {
	return call("Coordinator.TaskArrange", args, reply)
}

func CallForFinish(args *FinishArgs, reply *FinishReply) bool {
	return call("Coordinator.TaskFinish", args, reply)
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
