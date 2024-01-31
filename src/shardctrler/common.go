package shardctrler

import (
	"log"
	"time"
)

// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//
// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number，
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	OK                         = "OK"
	NoSuchConfig               = "NoSuchConfig"
	ErrNoKey                   = "ErrNoKey"
	ErrWrongLeader             = "ErrWrongLeader"
	ErrTimeout                 = "ErrTimeout"
	OpJoin         OperationOp = 1
	OpLeave        OperationOp = 2
	OpMove         OperationOp = 3
	OpQuery        OperationOp = 4
	ExecueTimeout              = 500 * time.Millisecond
)

type Err string
type OperationOp int

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type CommandRequest struct {
	Servers   map[int][]string // for JOIN  new GID -> servers mappings
	GIDs      []int            // for LEAVE
	Shard     int              // for Move
	GID       int              // for Move
	Num       int              // for Query
	Op        OperationOp
	ClientId  int64
	CommandId int64
}

type CommandReply struct {
	Err    Err
	Config Config
}

func DeepCopy(groups map[int][]string) map[int][]string {
	// 创建一个新的map用于存储深拷贝后的结果
	copiedGroups := make(map[int][]string)

	// 遍历原始map
	for key, value := range groups {
		// 创建一个新的切片用于存储对应键的值的深拷贝
		copiedValue := make([]string, len(value))

		// 将原始切片的元素逐个拷贝到新的切片中
		for i, v := range value {
			copiedValue[i] = v
		}

		// 将深拷贝后的切片放入新的map中
		copiedGroups[key] = copiedValue
	}

	return copiedGroups
}

type Command struct {
	Servers   map[int][]string // for JOIN  new GID -> servers mappings
	GIDs      []int            // for LEAVE
	Shard     int              // for Move
	GID       int              // for Move
	Num       int              // for Query
	Op        OperationOp
	ClientId  int64
	CommandId int64
}
