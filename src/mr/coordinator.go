package mr

import (
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// 让worker等待秒数
const (
	WAIT_SECONDS int    = 1
	BLANK_STRING string = ""
	NO_TASKS            = 0
	STILL_TASKS         = 1
)

var (
	NO_WORKER_ID   int = 0
	CRASH_DURATION int = 10
)

type Coordinator struct {
	// Your definitions here.
	files2WorkerId       map[string]int // map fileName -> workerId
	reduceId2WorkerId    map[int]int    // map reduceId -> workerId
	workerId             int            // 当前分配到的workerId
	status               int            // 状态 1map 2reduce 3done
	finishedTask         int            // 完成的任务数量
	nMap                 int            // Map任务的数量
	nReduce              int            // Reduce任务的数量
	successMapWokerId    []string       // 最后完成map任务的workerId 容错用
	successReduceWokerId []string       // 最后完成reduce任务的workerId 容错用
	timer                int            // 计时器的数量
	mux                  sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

func (c *Coordinator) WaitForOthers(args *Args, reply *Reply) error {
	reply.Response = WAIT_ORDER
	//reply.data = strconv.Itoa(WAIT_SECONDS)
	return nil
}

func (c *Coordinator) TaskFinish(args *Args, reply *Reply) error {
	reply.Response = FINISH_ORDER
	return nil
}

func (c *Coordinator) AssignMapTask(args *Args, reply *Reply) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	//// 给每个worker分配唯一的Id
	//if args.WorkerId == NO_WORKER_ID {
	//	c.workerId++
	//	reply.assignedId = c.workerId
	//} else {
	//	reply.assignedId = args.WorkerId
	//}

	if c.status == MAP_ORDER {
		taskAssigned := 0
		// 加锁，分配任务
		for key, value := range c.files2WorkerId {
			if value == NO_WORKER_ID {
				c.workerId++
				c.files2WorkerId[key], reply.AssignedId = c.workerId, c.workerId
				// 字符串深拷贝
				reply.Data = key
				taskAssigned = 1
				break
			}
		}

		if taskAssigned == STILL_TASKS {
			reply.Response = c.status
			// 开启一个异步任务，在10秒后执行指定的函数
			time.AfterFunc(10*time.Second, func() {
				c.mux.Lock()
				defer c.mux.Unlock()

				if c.files2WorkerId[reply.Data] != -1 {
					c.files2WorkerId[reply.Data] = 0
					fmt.Printf("Map任务" + reply.Data + "超时未完成，已经重置\n")
				}
			})
		} else {
			// 任务分配完成，让worker等待
			c.WaitForOthers(args, reply)
		}
	} else if c.status == REDUCE_ORDER {
		var taskAssigned int = 0
		for key, value := range c.reduceId2WorkerId {
			if value == NO_WORKER_ID {
				c.workerId++
				c.reduceId2WorkerId[key], reply.AssignedId = c.workerId, c.workerId
				reply.Data = strconv.Itoa(key)
				taskAssigned = 1
				break
			}
		}

		if taskAssigned == STILL_TASKS {
			reply.Response = c.status
			reply.SuccessMapWokerId = c.successMapWokerId
			// 开启一个异步任务，在10秒后执行指定的函数
			time.AfterFunc(10*time.Second, func() {
				c.mux.Lock()
				defer c.mux.Unlock()

				reduceId, _ := strconv.Atoi(reply.Data)
				if c.reduceId2WorkerId[reduceId] != -1 {
					c.reduceId2WorkerId[reduceId] = 0
					fmt.Printf("Map任务" + reply.Data + "超时未完成，已经重置\n")
				}
			})
		} else {
			// 任务分配完成，让worker等待
			c.WaitForOthers(args, reply)
		}
	} else if c.status == WAIT_ORDER {
		reply.Response = FINISH_ORDER
	}

	reply.NReduce = c.nReduce
	fmt.Printf("这边有数据 %v \n", reply)

	return nil
}

func (c *Coordinator) DeliveryTask(args *Args, reply *Reply) error {
	// 假设任务必定成功的情况下，采用一个计时器来模拟完成的任务数量
	c.mux.Lock()
	defer c.mux.Unlock()

	// map阶段，检测完成数量是否和map数量相同
	if c.status == MAP_ORDER {
		// 任被重新分配了，则不累加完成任务数量
		if c.files2WorkerId[args.Data] != args.AssignedId {
			return nil
		}
		// 排除无效交付后
		c.files2WorkerId[args.Data] = -1
		c.successMapWokerId = append(c.successMapWokerId, strconv.Itoa(args.AssignedId))
		c.finishedTask++
		fmt.Printf("任务" + args.Data + "已经交付\n")
		if c.finishedTask == c.nMap {
			c.status = REDUCE_ORDER
			c.finishedTask = 0

			//successMapWorkerId := []string{}
			//for _, value := range c.files2WorkerId {
			//	successMapWorkerId = append(successMapWorkerId, strconv.Itoa(value))
			//}
			//c.successMapWokerId = successMapWorkerId
			// 这里完成Map阶段，应该可以直接分配Reuce任务了吧
			//c.AssignMapTask(args, reply)
		} else {
			//c.AssignMapTask(args, reply)
		}
	} else if c.status == REDUCE_ORDER {
		if args.Response == MAP_ORDER {
			return nil
		}
		reduceId, _ := strconv.Atoi(args.Data)
		if c.reduceId2WorkerId[reduceId] != args.AssignedId {
			return nil
		}
		// 排除无效交付后
		c.reduceId2WorkerId[reduceId] = -1
		c.successReduceWokerId = append(c.successReduceWokerId, strconv.Itoa(args.AssignedId))
		c.finishedTask++
		if c.finishedTask == c.nReduce {
			c.status = WAIT_ORDER
			// 任务完成
			//c.TaskFinish(args, reply)
			reply.Response = WAIT_ORDER

			// 输出文件处理
			outputDir := "./"
			// 遍历目录
			err := filepath.Walk(outputDir, func(path string, info os.FileInfo, err error) error {
				// 忽略目录自身
				if path == outputDir {
					return nil
				}

				// 判断是否为文件
				if info.Mode().IsRegular() {
					// 获取文件名
					filename := filepath.Base(path)
					// 将属于这个reduce任务的文件，放入集合中
					words := strings.Split(filename, FILE_NAME_SPLITE)
					// 将最后完成对应reduce任务的文件重命名
					if words[0] == "mrfile" {
						if contain(c.successReduceWokerId, words[1]) {
							//fmt.Println("equals found!")
							newName := "mr-out-" + words[2]
							err := os.Rename(filename, newName)
							if err != nil {
								fmt.Println("Failed to rename file:", err)
							}
						} else {
							// 其余的无效执行直接删除
							err := os.Remove(filename)
							if err != nil {
								fmt.Println("Failed to delete file:", err)
							}
						}
					}
					//fmt.Println("reduce任务的文件名为" + filename)
				}

				return nil
			})

			if err != nil {
				fmt.Println(err)
			}

		} else {
			// 继续分配reduce任务
			//c.AssignMapTask(args, reply)
		}
	} else if c.status == WAIT_ORDER {
		// 任务完成
		c.finishedTask++
		c.TaskFinish(args, reply)
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
	ret := false

	// Your code here.
	c.mux.Lock()
	defer c.mux.Unlock()

	if c.status == WAIT_ORDER {
		ret = true
		// 临时文件夹处理
		os.RemoveAll("./temp") // clean up

	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	//fmt.Printf("有几个文件 %v ", files)
	c.mux.Lock()
	// 设置状态初始值为map阶段
	c.status = MAP_ORDER
	c.files2WorkerId = make(map[string]int)
	c.reduceId2WorkerId = make(map[int]int)

	for _, fileName := range files {
		c.nMap++
		c.files2WorkerId[fileName] = NO_WORKER_ID
	}
	for i := 0; i < nReduce; i++ {
		c.reduceId2WorkerId[i] = NO_WORKER_ID
	}
	c.nReduce = nReduce
	c.finishedTask = 0
	//fmt.Printf("有几个文件 %v ", c.files2WorkerId)
	c.mux.Unlock()

	c.server()
	return &c
}
