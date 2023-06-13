/*
 * @Author: ZeroOneTaT
 * @Date: 2023-06-11 13:42:53
 * @LastEditTime: 2023-06-13 22:05:35
 * @FilePath: /MIT-6.824/src/mr/coordinator.go
 * @Description: Coordinator
 *
 * Copyright (c) 2023 by ZeroOneTaT, All Rights Reserved.
 */
package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// 定义为全局锁，Worker之间访问Coordinator时加锁
var (
	mu sync.Mutex
)

// TaskMetaInfo	 任务元数据
type TaskMetaInfo struct {
	TaskState State     // 当前任务状态
	StartTime time.Time // 当前任务开启时间
	TaskAdr   *Task     // 当前任务指针，为的是这个任务从通道中取出来后，还能通过地址标记这个任务已经完成
}

// TaskMetaHolder 全部任务的元数据
type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo // 根据hash下标快速定位
}

// 协调者结构体
type Coordinator struct {
	// Your definitions here.
	ReducerNum        int            // 传入的参数决定需要多少个reducer
	TaskId            int            // 用于生成task的特殊id
	DistPhase         Phase          // 目前整个框架应该处于什么任务阶段
	ReduceTaskChannel chan *Task     // 使用chan保证并发安全
	MapTaskChannel    chan *Task     // 使用chan保证并发安全
	TaskMeta          TaskMetaHolder // 全部Task元数据
	Files             []string       // 传入的文件数组
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
/**
 * @description: 	所有任务完成，结束
 * @return {bool}
 */
func (c *Coordinator) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	if c.DistPhase == AllDone {
		fmt.Printf("All tasks are finished,the coordinator will be exit! !")
		return true
	} else {
		return false
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
/**
 * @description: 			Coordinator初始化函数
 * @param {[]string} files	文件数组
 * @param {int} nReduce		Reducer数量
 * @return {*}
 */
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:             files,
		ReducerNum:        nReduce,
		DistPhase:         MapPhase, // 最开始是 Map 任务
		MapTaskChannel:    make(chan *Task, len(files)),
		ReduceTaskChannel: make(chan *Task, nReduce),
		TaskMeta: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce), // Map + Reduce
		},
	}

	// 创建、分发 Map 任务
	c.makeMapTasks(files)

	// 启动服务器
	c.server()

	// 开启后台协程检测任务是否崩溃
	go c.CrashDetector()

	return &c
}

/**
 * @description: 监测任务是否崩溃
 * @return {*}
 */
func (c *Coordinator) CrashDetector() {
	for {
		time.Sleep(time.Second * 2) // 每隔2s检测一次

		mu.Lock() // 加锁，避免多个 goroutine 同时修改任务元数据信息

		// 全部任务执行完毕，退出监测
		if c.DistPhase == AllDone {
			mu.Unlock()
			break
		}

		// 遍历任务元信息
		for _, v := range c.TaskMeta.MetaMap {
			// 正常工作状态
			if v.TaskState == Working {
				fmt.Println("task[", v.TaskAdr.TaskId, "] is working: ", time.Since(v.StartTime), "s")
			}
			// 任务超时，超时时间：10 s
			if v.TaskState == Working && time.Since(v.StartTime) > time.Second*10 {
				fmt.Printf("the task[ %d ] is crash,take [%d] s\n", v.TaskAdr.TaskId, time.Since(v.StartTime))

				// 加入对应的任务队列等待重新调度
				switch v.TaskAdr.TaskType {
				case MapTask:
					{
						c.MapTaskChannel <- v.TaskAdr
						v.TaskState = Waiting
					}
				case ReduceTask:
					{
						c.ReduceTaskChannel <- v.TaskAdr
						v.TaskState = Waiting
					}
				}
			}
		}
		mu.Unlock() // 解锁
	}
}

/**
 * @description: 			生成 Map 任务
 * @param {[]string} files	文件数组
 * @return {*}
 */
func (c *Coordinator) makeMapTasks(files []string) {
	for _, file := range files {
		// 生成任务 ID
		id := c.generateTaskId()

		// 创建新任务
		task := Task{
			TaskType:   MapTask,
			TaskId:     id,
			ReducerNum: c.ReducerNum,
			FileSlice:  []string{file},
		}

		// 初始化任务
		taskMetaInfo := TaskMetaInfo{
			TaskState: Waiting, // 等待调度执行
			TaskAdr:   &task,   // 保存任务地址
		}

		// 任务信息保存
		c.TaskMeta.acceptMeta(&taskMetaInfo)

		// 加入任务队列
		c.MapTaskChannel <- &task
	}
}

/**
 * @description: TaskId 自增生成任务 ID
 * @return {*}
 */
func (c *Coordinator) generateTaskId() int {

	id := c.TaskId
	c.TaskId++
	return id
}

/**
 * @description: 						保存任务元信息到仓库
 * @param {*TaskMetaInfo} taskMetaInfo	当前任务元信息
 * @return {bool}						保存结果
 */
func (t *TaskMetaHolder) acceptMeta(taskMetaInfo *TaskMetaInfo) bool {
	taskId := taskMetaInfo.TaskAdr.TaskId
	meta, _ := t.MetaMap[taskId]

	if meta != nil {
		// 该任务已保存
		return false
	} else {
		// 该任务未保存，进行保存
		t.MetaMap[taskId] = taskMetaInfo
	}
	return true
}

/**
 * @description: 	生成 Reduce 任务
 * @return {*}
 */
func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateTaskId()
		task := Task{
			TaskId:    id,
			TaskType:  ReduceTask,
			FileSlice: selectReduceName(i),
		}

		// 保存任务的初始状态
		taskMetaInfo := TaskMetaInfo{
			TaskState: Waiting, // 任务等待被执行
			TaskAdr:   &task,   // 保存任务的地址
		}
		c.TaskMeta.acceptMeta(&taskMetaInfo)

		// 加入任务队列
		//fmt.Println("make a reduce task :", &task)
		c.ReduceTaskChannel <- &task
	}
}

/**
 * @description: 		根据 Reduce 任务数量选择对应的 Reduce 文件
 * @param {int} nReduce	Reduce 任务数量
 * @return {[]string}	文件数组
 */
func selectReduceName(reduceId int) []string {
	var files []string

	// 获取中间文件目录下的所有文件
	wd, _ := os.Getwd()
	path := filepath.Join(wd, "mr-tmp")
	allfiles, _ := ioutil.ReadDir(path)

	// 将对应的中间文件添加进来
	for _, file := range allfiles {
		if strings.HasPrefix(file.Name(), "mr-out-") && strings.HasSuffix(file.Name(), strconv.Itoa((reduceId))) {
			files = append(files, file.Name())
		}
	}

	// 返回最终文件
	return files
}

/**
 * @description: 			任务分发函数
 * @param {*TaskArgs} args	任务参数
 * @param {*Task} reply		执行任务
 * @return {*}				错误信息
 */
func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	// 上锁，防止多个Worker竞争
	mu.Lock()
	// 使用完自动解锁
	defer mu.Unlock()

	// 根据整体任务所处阶段进行任务分发
	switch c.DistPhase {
	// Map 阶段
	case MapPhase:
		{
			// 存在可用Map任务，从 c.MapTaskChannel 取出一个返回
			if len(c.MapTaskChannel) > 0 {
				*reply = *<-c.MapTaskChannel
				fmt.Printf("poll-Map-taskid[ %d ]\n", reply.TaskId)
				// 如果该任务当前正在运行，则打印一条消息
				if !c.TaskMeta.judgeState(reply.TaskId) {
					fmt.Printf("Map-taskid[ %d ] is running\n", reply.TaskId)
				}
			} else {
				// 不存在可用Map任务，返回  WaittingTask
				reply.TaskType = WaittingTask
				// 检查当前 Map 任务是否已经全部完成, 完成则进行状态切换
				if c.TaskMeta.checkTaskDone() {
					c.toNextPhase()
				}
			}
		}
	// Reduce 阶段
	case ReducePhase:
		{
			if len(c.ReduceTaskChannel) > 0 {
				*reply = *<-c.ReduceTaskChannel
				fmt.Printf("poll-Reduce-taskid[ %d ]\n", reply.TaskId)
				// 如果该任务当前正在运行，则打印一条消息
				if !c.TaskMeta.judgeState(reply.TaskId) {
					fmt.Printf("Reduce-taskid[ %d ] is running\n", reply.TaskId)
				}
			} else {
				// 如果没有可用的 Reduce 任务，返回 WaittingTask
				reply.TaskType = WaittingTask
				// 检查当前 Reduce 任务是否已经全部完成, 完成则进行状态切换
				if c.TaskMeta.checkTaskDone() {
					c.toNextPhase()
				}
				return nil
			}
		}
	// AllDone 阶段
	case AllDone:
		{
			// 所有任务完成，返回 ExitTask
			reply.TaskType = ExitTask
		}
	default:
		// 如果出现了未定义的阶段，则引发 panic 异常
		panic("The phase undefined ! ! !")

	}

	return nil
}

/**
 * @description: 整体任务状态切换函数
 * @return {*}
 */
func (c *Coordinator) toNextPhase() {
	if c.DistPhase == MapPhase {
		c.makeReduceTasks()
		c.DistPhase = ReducePhase
	} else if c.DistPhase == ReducePhase {
		c.DistPhase = AllDone
	}
}

/**
 * @description: 		检查任务工作状态, 如果任务处于 Waiting 则进行修改
 * @param {int} taskId	任务Id
 * @return {bool}		是否成功修改
 */
func (t *TaskMetaHolder) judgeState(taskId int) bool {
	taskInfo, ok := t.MetaMap[taskId]
	if !ok || taskInfo.TaskState != Waiting {
		return false
	}
	taskInfo.TaskState = Working
	taskInfo.StartTime = time.Now()
	return true
}

/**
 * @description: 	检查当前阶段任务是否全部完成
 * @return {bool}	完成 / 未完成
 */
func (t *TaskMetaHolder) checkTaskDone() bool {
	var (
		nMapDone      = 0
		nMapUnDone    = 0
		nReduceDone   = 0
		nReduceUnDone = 0
	)

	// 遍历 MetaMap
	for _, v := range t.MetaMap {
		// 判断任务类型
		if v.TaskAdr.TaskType == MapTask {
			// 判断任务是否完成
			if v.TaskState == Done {
				nMapDone++
			} else {
				nMapUnDone++
			}
		}
	}

	// 判断 Map / Reduce 任务是否全部完成
	if (nMapDone > 0 && nMapUnDone == 0) && (nReduceDone == 0 && nReduceUnDone == 0) {
		return true
	} else {
		if nReduceDone > 0 && nReduceUnDone == 0 {
			return true
		}
	}

	return false
}

/**
 * @description: 			标记任务完成
 * @param {*Task} args		任务参数
 * @param {*Task} reply		任务函数
 * @return {error}			调用错误
 */
func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	switch args.TaskType {
	case MapTask:
		{
			meta, ok := c.TaskMeta.MetaMap[args.TaskId]

			if ok && meta.TaskState == Working {
				meta.TaskState = Done
			} else {
				fmt.Printf("Map task Id[%d] is finished,already ! ! !\n", args.TaskId)
			}
		}
	case ReduceTask:
		{
			meta, ok := c.TaskMeta.MetaMap[args.TaskId]

			if ok && meta.TaskState == Working {
				meta.TaskState = Done
			} else {
				fmt.Printf("Reduce task Id[%d] is finished,already ! ! !\n", args.TaskId)
			}
		}
	default:
		panic("The task type undefined ! ! !")
	}

	return nil
}
