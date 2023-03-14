package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	nMap        int
	nReduce     int
	mapState    []int
	mapCnt      int
	reduceState []int
	reduceCnt   int
	files       []string
	mux         sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) checkTask(state *int) {
	time.Sleep(time.Duration(10) * time.Second)
	c.mux.Lock()
	defer c.mux.Unlock()
	if *state == Waiting {
		*state = NotArranged
	}
}

// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) TaskArrange(args *TaskArgs, reply *TaskReply) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	id := -1
	if c.mapCnt < c.nMap {
		for i, s := range c.mapState {
			if s == NotArranged {
				id = i
				break
			}
		}
		if id != -1 {
			reply.MapFile = c.files[id]
			reply.TaskType = MapTask
			reply.NReduce = c.nReduce
			c.mapState[id] = Waiting
			go c.checkTask(&c.mapState[id])
		}
	} else if c.reduceCnt < c.nReduce {
		for i, s := range c.reduceState {
			if s == NotArranged {
				id = i
				break
			}
		}
		if id != -1 {
			reply.TaskType = ReduceTask
			reply.NMap = c.nMap
			c.reduceState[id] = Waiting
			go c.checkTask(&c.reduceState[id])
		}
	} else {
		reply.TaskType = FinishTask
		return nil
	}
	if id == -1 {
		reply.TaskType = WaitTask
	} else {
		reply.TaskNumber = id
	}
	return nil
}

func (c *Coordinator) TaskFinish(args *FinishArgs, reply *FinishReply) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	if args.MapTaskNumber != -1 {
		c.mapState[args.MapTaskNumber] = Finished
		c.mapCnt++
	}
	if args.ReduceTaskNumber != -1 {
		c.reduceState[args.ReduceTaskNumber] = Finished
		c.reduceCnt++
	}
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
	c.mux.Lock()
	defer c.mux.Unlock()
	return c.reduceCnt == c.nReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nMap:        len(files),
		nReduce:     nReduce,
		mapState:    make([]int, len(files)),
		mapCnt:      0,
		reduceState: make([]int, nReduce),
		reduceCnt:   0,
		files:       files,
	}

	c.server()
	return &c
}
