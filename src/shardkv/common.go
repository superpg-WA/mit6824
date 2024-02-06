package shardkv

import (
	"6.5840/shardctrler"
	"fmt"
	"log"
	"time"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                                    = "OK"
	ErrNoKey                              = "ErrNoKey"
	ErrWrongGroup                         = "ErrWrongGroup"
	ErrWrongLeader                        = "ErrWrongLeader"
	ErrTimeout                            = "ErrTimeout"
	ErrOutDated                           = "ErrOutDated" // 尝试应用一个过时的配置
	ErrNotReady                           = "ErrNotReady"
	ExecuteTimeout                        = 500 * time.Millisecond
	Operation                 CommandType = 1                      // put append get command from clerk
	Configuration             CommandType = 2                      // new config
	ConfigureMonitorTimeout               = 100 * time.Millisecond // 100ms轮询一次shard controller
	InsertShards              CommandType = 3                      // insert shard
	MigrationMonitorTimeout               = 200 * time.Millisecond // 数据迁移延时
	DeleteShard               CommandType = 4                      // delete shard
	GCMonitorTimeout                      = 200 * time.Millisecond // GC时延
	EmptyEntry                CommandType = 5                      // Empty command to update the server state from crash before read
	EmptyEntryDetectorTimeout             = 100 * time.Millisecond
	OpGet                                 = "OpGet"
	OpPut                                 = "OpPut"
	OpAppend                              = "OpAppend"
)

type Err string
type CommandType uint8

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
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

// used by server applier and raft apply
type Command struct {
	Op   CommandType
	Data interface{}
}

func (command Command) String() string {
	return fmt.Sprintf("{Type:%v, Data:%v}", command.Op, command.Data)
}

func NewOperationCommand(request *CommandRequest) Command {
	return Command{Operation, *request}
}

func NewConfigurationCommand(config *shardctrler.Config) Command {
	return Command{Configuration, *config}
}

func NewInsertShardsCommand(response *ShardOperationReply) Command {
	return Command{InsertShards, *response}
}

func NewDeleteShardsCommand(request *ShardOperationRequest) Command {
	return Command{DeleteShard, *request}
}

// 分片操作请求
type ShardOperationRequest struct {
	// 配置版本号
	ConfigNum int
	// 分片序列号
	ShardIds []int
}

// 分片操作返回值
type ShardOperationReply struct {
	// 分片内容
	Shards map[int]map[string]string
	// 去重map
	LastOperation map[int64]OperationContext
	// 配置版本号
	ConfigNum int
	Err       Err
}

func NewEmptyCommand() Command {
	return Command{EmptyEntry, nil}
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

func (reply *CommandReply) DeepCopy() *CommandReply {
	ret := new(CommandReply)
	ret.Value = reply.Value
	ret.Err = reply.Err
	return ret
}

/*
*
Shard分片
*/
type ShardStatus uint8

const (
	Serving     ShardStatus = 1
	Pulling     ShardStatus = 2
	BePulleding ShardStatus = 3
	GCing       ShardStatus = 4
)

type Shard struct {
	KV     map[string]string
	Status ShardStatus
}

func NewShard() *Shard {
	return &Shard{make(map[string]string), Serving}
}

func (shard *Shard) Get(key string) (string, Err) {
	if value, ok := shard.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (shard *Shard) Put(key, value string) Err {
	shard.KV[key] = value
	return OK
}

func (shard *Shard) Append(key, value string) Err {
	shard.KV[key] += value
	return OK
}

func (shard *Shard) DeepCopy() map[string]string {
	newShard := make(map[string]string)
	for k, v := range shard.KV {
		newShard[k] = v
	}
	return newShard
}
