package kvraft

import "time"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
	OpGet          = "OpGet"
	OpPut          = "OpPut"
	OpAppend       = "OpAppend"
	ExecueTimeout  = 500 * time.Millisecond
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type CommandRequest struct {
	Key   string
	Value string
	Op    string
	// 为保证幂等性，不会同一条日志apply多次，需要用(ClinetId, CommandId)来唯一标志一个请求命令
	ClientId  int64
	CommandId int64
}

type CommandReply struct {
	Err   Err
	Value string
}

type Command struct {
	Key   string
	Value string
	Op    string
	// 为保证幂等性，不会同一条日志apply多次，需要用(ClinetId, CommandId)来唯一标志一个请求命令
	ClientId  int64
	CommandId int64
}
