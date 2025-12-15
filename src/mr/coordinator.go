package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	Files       []string
	NReduce     int
	MapTasks    []TaskStatus
	ReduceTasks []TaskStatus
	Phase       Phase

	Mutex   sync.Mutex
	Timeout int64
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	// 逻辑：根据当前阶段，分配空闲的Map/Reduce任务；无空闲则返回Wait
	// - MapPhase：找Idle的Map任务，标记为InProgress，记录开始时间
	// - ReducePhase：找Idle的Reduce任务，标记为InProgress，记录开始时间
	// - DonePhase：返回任务完成
	switch c.Phase {
	case MapPhase:
		// 1. 收集所有Idle的Map任务ID
		for i, task := range c.MapTasks {
			if task.Status == Idle {
				c.MapTasks[i].Status = InProgress
				c.MapTasks[i].StartTime = NowUnix()
				reply.TaskType = "Map"
				reply.TaskID = i
				reply.Files = []string{c.Files[i]}
				reply.NReduce = c.NReduce
				// log.Printf("Assign Map task %d to Worker", i)
				return nil
			}
		}
		reply.TaskType = "Wait"
		return nil

	case ReducePhase:
		for i, task := range c.ReduceTasks {
			if task.Status == Idle {
				c.ReduceTasks[i].Status = InProgress
				c.ReduceTasks[i].StartTime = NowUnix()
				reply.TaskType = "Reduce"
				reply.TaskID = i
				reply.NReduce = c.NReduce
				// log.Printf("Assign Reduce task %d to Worker", i)
				return nil
			}
		}
		reply.TaskType = "Wait"
		return nil
	case DonePhase:
		reply.TaskType = "Done"
		return nil
	default:
		return nil
	}
}

// 2. Worker上报任务完成
func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	// 逻辑：根据任务类型（Map/Reduce），将任务状态标记为Done
	// - Map任务完成：需记录其输出的中间文件路径（供Reduce任务读取）
	// - 所有Map任务完成后，切换到ReducePhase
	// log.Printf("Worker report %s task  %d done", args.TaskType, args.TaskID)
	reply.Success = true
	if args.TaskType == "Map" {
		taskID := args.TaskID
		if c.MapTasks[taskID].Status != InProgress {
			return nil
		}
		c.MapTasks[taskID].Status = Done
		c.MapTasks[taskID].MapOutputs = args.MapOutputs
		allMapDone := true
		for _, task := range c.MapTasks {
			if task.Status != Done {
				allMapDone = false
				break
			}
		}
		if allMapDone {
			c.Phase = ReducePhase
			log.Println("All Map tasks done → switch to ReducePhase")
		}
	} else if args.TaskType == "Reduce" {
		taskID := args.TaskID
		if c.ReduceTasks[taskID].Status != InProgress {
			return nil
		}
		c.ReduceTasks[taskID].Status = Done
		allReduceDone := true
		for _, task := range c.ReduceTasks {
			if task.Status != Done {
				allReduceDone = false
				break
			}
		}
		if allReduceDone {
			c.Phase = DonePhase
			log.Println("All Reduce tasks done → switch to DonePhase")
		}
	}
	return nil
}

// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

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
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	return c.Phase == DonePhase
}

// checkTimeout：后台goroutine，每秒检测一次超时任务
func (c *Coordinator) checkTimeOut() {
	for {
		time.Sleep(100 * time.Millisecond)
		c.Mutex.Lock()
		if c.Phase == DonePhase {
			c.Mutex.Unlock()
			return
		}
		switch c.Phase {
		case MapPhase:
			for i, task := range c.MapTasks {
				if task.Status == InProgress && NowUnix()-task.StartTime > c.Timeout {
					log.Printf("Map task %d timeout, reset to Idle", i)
					c.MapTasks[i].Status = Idle
					for j := 0; j < c.NReduce; j++ {
						tempFile := fmt.Sprintf("mr-%d-%d.tmp", i, j)
						os.Remove(tempFile)
					}
				}
			}
		case ReducePhase:
			for i, task := range c.ReduceTasks {
				if task.Status == InProgress && NowUnix()-task.StartTime > c.Timeout {
					log.Printf("Reduce task %d timeout, reset to Idle", i)
					c.ReduceTasks[i].Status = Idle
					tempOutFile := fmt.Sprintf("mr-out-%d.tmp", i)
					os.Remove(tempOutFile)
				}
			}
		}
		c.Mutex.Unlock()
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:   files,
		NReduce: nReduce,
		Phase:   MapPhase,
		Timeout: 10000,
	}
	c.MapTasks = make([]TaskStatus, len(files))
	for i := range c.MapTasks {
		c.MapTasks[i] = TaskStatus{Status: Idle}
	}
	c.ReduceTasks = make([]TaskStatus, nReduce)
	for i := range c.ReduceTasks {
		c.ReduceTasks[i] = TaskStatus{Status: Idle}
	}
	// Your code here.

	c.server()
	go c.checkTimeOut()
	return &c
}
