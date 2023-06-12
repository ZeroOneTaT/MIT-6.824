/*
 * @Author: ZeroOneTaT
 * @Date: 2023-06-11 13:42:53
 * @LastEditTime: 2023-06-12 14:31:34
 * @FilePath: /MIT-6.824/src/mr/rpc.go
 * @Description: RPC
 *
 * Copyright (c) 2023 by ZeroOneTaT, All Rights Reserved.
 */
package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
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

// TaskArgs RPC 应该传入的参数，可实际上应该什么都不用传,因为只是Worker获取一个任务
type TaskArgs struct{}

// TaskType 对于下方枚举任务的父类型
type TaskType int

// Phase 对于分配任务阶段的父类型
type Phase int

// State 任务的状态的父类型
type State int

// 枚举任务的类型
const (
	MapTask TaskType = iota
	ReduceTask
	WaittingTask // Waittingen任务代表此时为任务都分发完了，但是任务还没完成，阶段未改变
	ExitTask     // exit
)

// 枚举阶段的类型
const (
	MapPhase    Phase = iota // 此阶段在分发MapTask
	ReducePhase              // 此阶段在分发ReduceTask
	AllDone                  // 此阶段已完成
)

// 任务状态类型
const (
	Working State = iota // 此阶段在工作
	Waiting              // 此阶段在等待执行
	Done                 // 此阶段已经做完
)

// Task 结构体
// Worker向 Coordinator 获取 Task 的结构体
type Task struct {
	TaskType   TaskType // 任务类型: Map / Reduce
	TaskId     int      // 任务id
	ReducerNum int      // Reducer的数量，用于hash
	FileSlice  []string // 输入文件的切片，Map一个文件对应一个文件，Reduce是对应多个temp中间值文件
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
