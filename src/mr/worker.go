package mr

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

const (
	MAP_ORDER        int    = 1
	REDUCE_ORDER     int    = 2
	WAIT_ORDER       int    = 3
	FINISH_ORDER     int    = 4
	PATH_PREFIX      string = ""
	TEMP_FILE_PATH   string = "./temp/"
	WRX              int    = 0750
	TXT_FILE         string = ".txt"
	FILE_NAME_SPLITE string = "-"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

var workerId = 0

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
	askForTask(mapf, reducef)

}

/*
worker 发送的消息类型：
1. 请求map任务 (定时发送请求，但是worker不知道自己要执行什么任务)
2. 完成map任务
3. 请求reduce任务
4. 完成reduce任务
5. ping （不需要，没有主容错）

coordinator 发送的消息类型：
1. 分配map任务（不包含文件） 文件在本实验中是在worker本地的。（实际是GFS）。
2. 分配reduce任务
3. pong （不需要，没有主容错）

RPC请求的数据结构设计如下：
{
	type int, // 1请求任务 2分配任务 3完成任务
	task string, // 任务类型 map or reduce
	Id int, // 任务id
}

优化了一下，发现有一些过度设计，优化结果如下：

coordinator的请求返回如下：
1. worker请求任务：
	1.1 分配map任务
	1.2 分配reduce任务
	1.3 批处理等待
2. work完成任务：
	1.1 分配map任务
	1.2 分配reduce任务
	1.3 批处理等待

RPC请求的数据结构设计如下：
{
	request int;
}

*/

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.

func askForTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	args := Args{}
	args.Request = 1
	args.WorkerId = workerId
	reply := Reply{}
	reply.Response = 0

	for reply.Response < FINISH_ORDER {
		ok := call("Coordinator.AssignMapTask", &args, &reply)
		fmt.Printf("reply内容 %v \n", reply)

		if ok {
			// reply.Y should be 100.
			//fmt.Printf("reply.Y %v\n", reply.Y)
			// worker等待
			if reply.Response == MAP_ORDER {
				mapTask(&reply, mapf)
				args.Data = reply.Data
				args.AssignedId = reply.AssignedId
				args.Request = reply.Response
				call("Coordinator.DeliveryTask", &args, &reply)
			} else if reply.Response == REDUCE_ORDER {
				reduceTask(&reply, reducef)
				args.Data = reply.Data
				args.AssignedId = reply.AssignedId
				args.Request = reply.Response
				call("Coordinator.DeliveryTask", &args, &reply)
				if reply.Response == FINISH_ORDER {
					break
				}
			} else if reply.Response == WAIT_ORDER {
				// todo: 等待逻辑
				time.Sleep(1 * time.Second)
				fmt.Printf("任务尚未结束，我的主人需要我。等待 %v 秒钟，继续发起请求。\n", reply.Data)
			}
		} else {
			fmt.Printf("RPC请求调用失败！Worker无法找到Coordinator，即将退出！\n")
			break
		}
	}
}

func contain(list []string, word string) bool {
	for _, item := range list {
		if item == word {
			return true
		}
	}
	return false
}

func reduceTask(reply *Reply, reducef func(string, []string) string) {
	tmpDir := TEMP_FILE_PATH
	fileNames := []string{}

	fmt.Printf("完成的map任务Id: %v \n", reply.SuccessMapWokerId)

	// 遍历目录
	err := filepath.Walk(tmpDir, func(path string, info os.FileInfo, err error) error {
		// 忽略目录自身
		if path == tmpDir {
			return nil
		}

		// 判断是否为文件
		if info.Mode().IsRegular() {
			// 获取文件名
			filename := filepath.Base(path)
			// 将属于这个reduce任务的文件，放入集合中
			words := strings.Split(filename, FILE_NAME_SPLITE)
			if words[2] == reply.Data && contain(reply.SuccessMapWokerId, words[1]) {
				//fmt.Println("equals found!")
				fileNames = append(fileNames, filename)
			}
			//fmt.Println("reduce任务的文件名为" + filename)
		}

		return nil
	})

	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("文件名:%v\n", fileNames)

	intermediate := []KeyValue{}

	var fileKey string
	var fileValue string
	// 读取本reduce任务需要处理的文件，放入intermediate中，并排序
	for _, fileName := range fileNames {
		fileName = TEMP_FILE_PATH + fileName
		file, _ := os.Open(fileName)
		//fmt.Fscanf(file, "%s %s\n", &fileKey, &fileValue)

		// 创建Scanner对象
		scanner := bufio.NewScanner(file)

		// 循环读取文件中的每一行
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Sscanf(line, "%s %s\n", &fileKey, &fileValue)
			intermediate = append(intermediate, KeyValue{fileKey, fileValue})
			//fmt.Println(line)
		}

		//// 检查是否发生错误
		//if err := scanner.Err(); err != nil {
		//	fmt.Println(err)
		//}

		//fmt.Printf("%s %s\n", fileKey, fileValue)

	}

	sort.Sort(ByKey(intermediate))
	//fmt.Printf("中间文件内容: %v\n", intermediate)

	// 结果文件
	ofileName := "./mrfile-" + strconv.Itoa(reply.AssignedId) + "-" + reply.Data

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

		// 将结果写入文件中
		ofile, err := os.OpenFile(ofileName, os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			ofile, _ = os.Create(ofileName)
		}
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
}

func mapTask(reply *Reply, mapf func(string, string) []KeyValue) {
	// 文件名，拼接相对路径
	filename := PATH_PREFIX + reply.Data

	fmt.Printf("有几个文件 %v \n", reply)

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	//kva := mapf(filename, string(content))

	//fmt.Printf("读取内容 %v \n", content)

	// 生成临时文件夹，并存储临时文件
	os.Mkdir(TEMP_FILE_PATH, os.FileMode(WRX))

	intermediate := []KeyValue{}
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	// 中间文件名称前缀文件格式 mr-mapTaskId-reduceTaskId
	interFileNamePrefix := TEMP_FILE_PATH + "mr-" + strconv.Itoa(reply.AssignedId)
	sort.Sort(ByKey(intermediate))
	for _, item := range intermediate {
		reduceTaskId := ihash(item.Key) % reply.NReduce
		interFileName := interFileNamePrefix + "-" + strconv.Itoa(reduceTaskId)
		// 追加方式写入文件
		file, err := os.OpenFile(interFileName, os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			file, _ = os.Create(interFileName)
		}
		fmt.Fprintf(file, "%v %v\n", item.Key, item.Value)
	}
	//defer os.RemoveAll(dir) // clean up

}

//func CallExample() {
//
//	// declare an argument structure.
//	args := ExampleArgs{}
//
//	// fill in the argument(s).
//	args.kind = 99
//
//	// declare a reply structure.
//	reply := ExampleReply{}
//
//	// send the RPC request, wait for the reply.
//	// the "Coordinator.Example" tells the
//	// receiving server that we'd like to call
//	// the Example() method of struct Coordinator.
//	ok := call("Coordinator.Example", &args, &reply)
//	if ok {
//		// reply.Y should be 100.
//		fmt.Printf("reply.Y %v\n", reply.Y)
//	} else {
//		fmt.Printf("call failed!\n")
//	}
//}

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
