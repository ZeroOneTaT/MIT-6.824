/*
 * @Author: ZeroOneTaT
 * @Date: 2023-06-11 13:42:53
 * @LastEditTime: 2023-06-13 23:46:52
 * @FilePath: /MIT-6.824/src/mr/worker.go
 * @Description: Woker 实现
 *
 * Copyright (c) 2023 by ZeroOneTaT, All Rights Reserved.
 */
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

// 自定义排序, 重写 Len, Swap, Less
type byKeyValue []KeyValue

func (k byKeyValue) Len() int           { return len(k) }
func (k byKeyValue) Swap(i, j int)      { k[i], k[j] = k[j], k[i] }
func (k byKeyValue) Less(i, j int) bool { return k[i].Key < k[j].Key }

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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	taskLoop := true // Worker循环标志

	for taskLoop {
		task := GetTask()
		switch task.TaskType {
		case MapTask:
			{
				// 执行 Map
				DoMapTask(mapf, &task)
				callDone(&task)
			}
		case ReduceTask:
			{
				// 执行 Reduce
				DoReduceTask(reducef, &task)
				callDone(&task)
			}
		case WaittingTask:
			{
				time.Sleep(time.Second * 5)
				//fmt.Println("All tasks are in progress, please wait...")
			}
		case ExitTask:
			{
				time.Sleep(time.Second)
				fmt.Println("All tasks are Done ,will be exiting...")
				taskLoop = false
			}
		}
	}

	time.Sleep(time.Second)
}

// 调用 RPC 获取任务： Map Task / Reduce Task
func GetTask() Task {
	args := TaskArgs{}
	reply := Task{}
	// 调用RPC
	ok := call("Coordinator.PollTask", &args, &reply)

	if ok {
		fmt.Println("Worker get ", reply.TaskType, "Task :Id[", reply.TaskId, "]")
	} else {
		fmt.Println("RPC call failed!")
	}

	return reply
}

/**
 * @description: 								Map 任务执行函数
 * @param {func(string, string)} [] KeyValue	实际 Map 函数
 * @param {*Task} response						当前任务参数
 * @return {*}
 */
func DoMapTask(mapf func(string, string) []KeyValue, response *Task) {
	var intermediate []KeyValue
	filename := response.FileSlice[0]

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Cannot open %v", filename)
	}
	// 读取文件
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Cannot read %v", filename)
	}
	file.Close()

	// mapf 返回一组kv数组
	intermediate = mapf(filename, string(content))

	// Reducer 数量
	n_reduce := response.ReducerNum

	// 创建 n_reduce 长度的切片
	hashkv := make([][]KeyValue, n_reduce)

	// 哈希映射, 保证相同的 key 分配到相同的 Reducer
	for _, kv := range intermediate {
		hashkv[ihash(kv.Key)%n_reduce] = append(hashkv[ihash(kv.Key)%n_reduce], kv)
	}

	// // 使用临时文件, 使用json编码，防止进程由于某些原因退出了，产生不完整的文件
	// tmpFiles := make([]*os.File, n_reduce)
	// for i := 0; i < n_reduce; i++ {
	// 	// tmpFiles[i], _ = ioutil.TempFile("mr-tmp", "mr-tmp-*")
	// 	tmpFiles[i], _ = ioutil.TempFile("mr-tmp", "mr-tmp-*")
	// 	encoder := json.NewEncoder(tmpFiles[i])
	// 	for _, kv := range hashkv[i] {
	// 		log.Fatalln(kv)
	// 		err := encoder.Encode(kv)
	// 		if err != nil {
	// 			log.Fatal("Failed to encoder mapfile ", err)
	// 		}
	// 	}

	// }
	// // 写入最终的中间文件
	// for outindex, file := range tmpFiles {
	// 	outname := "mr-out-" + strconv.Itoa(response.TaskId) + "-" + strconv.Itoa(outindex)
	// 	tmppath := filepath.Join(file.Name())
	// 	os.Rename(tmppath, outname)
	// 	// file.Close()
	// }

	for i := 0; i < n_reduce; i++ {
		oname := "mr-tmp-" + strconv.Itoa(response.TaskId) + "-" + strconv.Itoa(i)
		ofile, _ := os.Create(oname)
		enc := json.NewEncoder(ofile)
		for _, kv := range hashkv[i] {
			err := enc.Encode(kv)
			if err != nil {
				log.Fatal("Failed to encoder mapfile ", err)
				return
			}
		}
		ofile.Close()
	}
}

/**
 * @description: 									Reduce 任务执行函数
 * @param {func(string, []string) string)} string	实际 Reduce 函数
 * @param {*Task} response							当前任务参数
 * @return {*}
 */
func DoReduceTask(reducef func(string, []string) string, response *Task) {
	n_reduce_file := response.TaskId
	// 对传入的 KeyValue 进行 json 解码并排序
	intermediate := shuffle(response.FileSlice)
	dir, _ := os.Getwd()
	tmpFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	for i := 0; i < len(intermediate); {
		// 寻找相同的 Key
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		// 合并相同的 Key
		var samekv []string
		for k := i; k < j; k++ {
			samekv = append(samekv, intermediate[k].Value)
		}

		// 调用 Reduce 函数统计
		output := reducef(intermediate[i].Key, samekv)

		// 写入临时文件
		fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tmpFile.Close()

	// 重命名
	outname := fmt.Sprintf("mr-out-%d", n_reduce_file)
	os.Rename(tmpFile.Name(), outname)
}

/**
 * @description: 			类似于MapReduce论文中的洗牌方法，对KeyValue数组排序
 * @param {[]string} files	保存文件名
 * @return {*}				排序好的KeyValue数组
 */
func shuffle(files []string) []KeyValue {
	var sorted_kv []KeyValue

	// 正确的 MapReduce 需要调用 RPC 从 GFS 中读取文件
	// 此处为简化版， 文件保存在 Worker 机器上
	// json 文件模拟网络传输的文件
	for _, filepath := range files {
		file, _ := os.Open(filepath)

		decoder := json.NewDecoder(file)

		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			sorted_kv = append(sorted_kv, kv)
		}
		file.Close()
	}
	// 排序
	sort.Sort(byKeyValue(sorted_kv))

	return sorted_kv
}

/**
 * @description: 调用 RPC 标记任务已经完成
 * @param {*Task} finish
 * @return {*} Task
 */
func callDone(finish *Task) Task {
	args := finish
	reply := Task{}
	ok := call("Coordinator.MarkFinished", &args, &reply)

	if ok {
		log.Fatalln("Worker finish :taskId[", args.TaskId, "]")
		// fmt.Println("Worker finish :taskId[", args.TaskId, "]")
	} else {
		fmt.Println("RPC call failed!")
	}

	return reply
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
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":9906")
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
