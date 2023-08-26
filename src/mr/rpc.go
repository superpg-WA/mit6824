package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

//RPC请求的数据结构设计如下：
//{
//kind int; // 1请求任务 2分配任务 3完成任务 4Ping 5Pong
//task string; // 任务类型 map or reduce
//id int // 任务id
//}

type Args struct {
	Request    int    // 1 请求任务 2 完成任务
	WorkerId   int    // 发起请求的workerId（唯一）
	Data       string // 交付的任务 map文件名 reduce为任务id
	AssignedId int    // 被分配的唯一workerId
	Response   int    // 1 分配map任务 2 分配reduce任务 3 等待
	// 新意义：map任务的Id和reduce任务的id
}

type Reply struct {
	Data              string   // map任务文件名 Reduce任务的任务Id or 等待秒数 传输时使用byte
	Response          int      // 1 分配map任务 2 分配reduce任务 3 等待
	AssignedId        int      // 被分配的唯一workerId
	NReduce           int      // reduce任务的数量
	TaskId            int      // 任务Id map 或 reduce 任务中使用
	SuccessMapWokerId []string // 最后完成map任务的workerId 容错用
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
