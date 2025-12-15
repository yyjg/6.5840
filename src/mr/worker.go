package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// workerID := rand.Intn(1000)
	// log.Printf("Worker %d started", workerID)
	// Your worker implementation here.
	for {
		args := AskTaskArgs{}
		reply := AskTaskReply{}
		ok := call("Coordinator.AskTask", &args, &reply)
		if !ok {
			log.Println("Call AskTask failed, sleep 1s")
			time.Sleep(1 * time.Second)
			continue
		}
		switch reply.TaskType {
		case "Map":
			doMapTask(reply.TaskID, reply.Files[0], reply.NReduce, mapf)
			time.Sleep(1 * time.Second)
			// 上报任务完成
			reportTaskDone("Map", reply.TaskID)
			clearMapOutputs(reply.TaskID)
		case "Reduce":
			doReduceTask(reply.TaskID, reply.NReduce, reducef)
			time.Sleep(1 * time.Second)
			// 上报任务完成
			reportTaskDone("Reduce", reply.TaskID)
			clearMapOutputs(reply.TaskID)
		case "Wait":
			// 无任务可做，休眠1秒后重试
			time.Sleep(1 * time.Second)
		case "Done":
			log.Println("All tasks done, Worker exit")
			return
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Printf("dialing failed: %v, will retry", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}

func reportTaskDone(taskType string, taskID int) {
	args := TaskDoneArgs{
		TaskType: taskType,
		TaskID:   taskID,
	}
	if taskType == "Map" {
		args.MapOutputs = getMapOutputs(taskID)
	}
	reply := TaskDoneReply{}
	for i := 0; i < 3; i++ {
		if call("Coordinator.TaskDone", &args, &reply) {
			return
		}
		time.Sleep(1 * time.Second)
	}
	log.Printf("Report %s task %d done failed", taskType, taskID)
}

// doMapTask：执行Map任务
// taskId：Map任务ID；filename：输入文件；nReduce：Reduce总数；mapf：用户定义的Map函数
func doMapTask(taskID int, fileName string, nReduce int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("Worker: open file %s failed: %v", fileName, err)
	}
	content, err := io.ReadAll(file)
	file.Close()
	if err != nil {
		log.Fatalf("Worker: read file %s failed: %v", fileName, err)
	}
	kva := mapf(fileName, string(content))

	reduceWriters := make([]*json.Encoder, nReduce)

	tempFiles := make([]string, nReduce)

	for i := range nReduce {
		tempFile := fmt.Sprintf("mr-%d-%d.tmp", taskID, i)
		os.Remove(tempFile)
		f, err := os.Create(tempFile)
		if err != nil {
			log.Fatalf("Worker: create temp file %s failed: %v", tempFile, err)
		}
		defer f.Close()
		tempFiles[i] = tempFile
		reduceWriters[i] = json.NewEncoder(f)
	}
	for _, kv := range kva {
		reduceID := ihash(kv.Key) % nReduce
		err := reduceWriters[reduceID].Encode(&kv)
		if err != nil {
			log.Fatalf("Worker: encode kv %v failed: %v", kv, err)
		}
	}
	saveMapOutputs(taskID, tempFiles)
	log.Printf("Worker: Map task %d done, output to %v", taskID, tempFiles)
}

// doReduceTask：执行Reduce任务
// taskId：Reduce任务ID；nReduce：Reduce总数；reducef：用户定义的Reduce函数
func doReduceTask(taskID int, nReduce int, reducef func(string, []string) string) {
	var kva []KeyValue
	for mapID := 0; ; mapID++ {
		tempFile := fmt.Sprintf("mr-%d-%d.tmp", mapID, taskID)
		if _, err := os.Stat(tempFile); os.IsNotExist(err) {
			break
		}
		file, err := os.Open(tempFile)
		if err != nil {
			log.Fatalf("Worker: open temp file %s failed: %v", tempFile, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	// 3. 调用用户定义的Reduce函数（如wc.so的Reduce）
	// 最终输出文件：mr-out-{reduceId}
	tempOutFile := fmt.Sprintf("mr-out-%d.tmp", taskID)
	os.Remove(tempOutFile)
	f, err := os.Create(tempOutFile)
	if err != nil {
		log.Fatalf("Worker: create out file %s failed: %v", tempOutFile, err)
	}
	defer f.Close()

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(f, "%v %v\n", kva[i].Key, output)
		i = j
	}
	outFile := fmt.Sprintf("mr-out-%d", taskID)
	os.Rename(tempOutFile, outFile)
	for mapID := 0; ; mapID++ {
		tempFile := fmt.Sprintf("mr-%d-%d.tmp", mapID, taskID)
		if _, err := os.Stat(tempFile); os.IsNotExist(err) {
			break
		}
		os.Remove(tempFile)
	}
	log.Printf("Worker: Reduce task %d done, output to %s", taskID, outFile)
}

// 辅助函数：保存Map任务输出文件路径（Worker本地临时存储）
var mapOutputsCache = make(map[int][]string) // taskId → 输出文件列表
var cacheMu sync.Mutex

func saveMapOutputs(taskId int, outputs []string) {
	cacheMu.Lock()
	mapOutputsCache[taskId] = outputs
	cacheMu.Unlock()
}

func getMapOutputs(taskId int) []string {
	cacheMu.Lock()
	defer cacheMu.Unlock()
	return mapOutputsCache[taskId]
}

// 新增：清理cache，避免内存泄漏
func clearMapOutputs(taskId int) {
	cacheMu.Lock()
	delete(mapOutputsCache, taskId)
	cacheMu.Unlock()
}
