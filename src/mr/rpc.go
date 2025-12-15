package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

// example to show how to declare the arguments
// and reply for an RPC.
// 单个任务的状态
type TaskStatus struct {
	Status     TaskState // 空闲/运行中/已完成
	WorkerID   int
	StartTime  int64 // 任务开始时间（用于检测超时）
	MapOutputs []string
}

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	DonePhase
)

type TaskState int

const (
	Idle TaskState = iota
	InProgress
	Done
)

type AskTaskArgs struct{}
type AskTaskReply struct {
	Files    []string
	TaskID   int
	TaskType string
	NReduce  int
}

type TaskDoneArgs struct {
	MapOutputs []string
	TaskType   string
	TaskID     int
}
type TaskDoneReply struct {
	Success bool
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func NowUnix() int64 {
	return time.Now().UnixMilli()
}
