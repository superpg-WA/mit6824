package shardctrler

import (
	"6.5840/labgob"
	"6.5840/raft"
	"fmt"
	"sort"
	"time"
)
import "6.5840/labrpc"
import "sync"

type ShardCtrler struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	//configs []Config // indexed by config num

	lastApplied    int // 记录当前最后一个被应用的日志，以阻止状态机回滚
	cf             ShardController
	lastOperations map[int64]OperationContext // 客户端id 2 具体操作，用于判断是否请求是否重复
	notifyChans    map[int]chan *CommandReply // 用于返回请求结果的消息管道
}

type OperationContext struct {
	LastReply *CommandReply
	CommandId int64
}

type ShardController interface {
	Join(groups map[int][]string) Err
	Leave(gids []int) Err
	Move(shard, gid int) Err
	Query(num int) (Config, Err)
}

// 配置状态机的内存实现版本
// 只包装了基本的map操作，无法保证并发可用
type MemoryConfigStateMachine struct {
	Configs []Config
}

func NewMemoryConfigStateMachine() *MemoryConfigStateMachine {
	configs := make([]Config, 1)
	configs[0].Groups = map[int][]string{}
	return &MemoryConfigStateMachine{
		Configs: configs,
	}
}

// 加入一些raft组，移动分片
func (cf *MemoryConfigStateMachine) Join(groups map[int][]string) Err {
	//DPrintf("Join args: %v.", groups)
	lastConfig := cf.Configs[len(cf.Configs)-1]
	newConfig := Config{len(cf.Configs), lastConfig.Shards, DeepCopy(lastConfig.Groups)}

	for gid, servers := range groups {
		// 如果没有当前这个gid，直接赋值
		if _, ok := newConfig.Groups[gid]; !ok {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			newConfig.Groups[gid] = newServers
		}
	}

	// gid -> shards
	s2g := Group2Shards(newConfig)
	//DPrintf("join op s2g: %v.", s2g)

	for {
		// 每次选择分片数最多的raft组和分片数最少的raft组，移动一个，保证动态平衡
		source, target := GetGIDWithMaximumShards(s2g), GetGIDWithMinimumShards(s2g)
		if source != 0 && len(s2g[source])-len(s2g[target]) <= 1 {
			break
		}
		s2g[target] = append(s2g[target], s2g[source][0])
		s2g[source] = s2g[source][1:]
	}

	//DPrintf("join op s2g: %v.", s2g)

	// shard -> gid 的映射
	var newShards [NShards]int
	for gid, shards := range s2g {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}
	newConfig.Shards = newShards
	cf.Configs = append(cf.Configs, newConfig)
	return OK
}

func Group2Shards(config Config) map[int][]int {
	ret := make(map[int][]int)
	for shard, gid := range config.Shards {
		if _, ok := ret[gid]; !ok {
			ret[gid] = make([]int, 0)
		}
		ret[gid] = append(ret[gid], shard)
	}

	// 补全新增的gid
	for gid := range config.Groups {
		if _, ok := ret[gid]; !ok {
			ret[gid] = make([]int, 0)
		}
	}
	return ret
}

// 需要确保迭代是确定性的
func GetGIDWithMinimumShards(s2g map[int][]int) int {
	// 将map切换为数组
	var keys []int
	for k := range s2g {
		keys = append(keys, k)
	}
	// 按照gid从小到大的顺序进行迭代
	sort.Ints(keys)
	index, min := -1, NShards+1
	for _, gid := range keys {
		if gid != 0 && len(s2g[gid]) < min {
			index, min = gid, len(s2g[gid])
		}
	}
	return index
}

// 需要确保迭代是确定性的
func GetGIDWithMaximumShards(s2g map[int][]int) int {
	// 0 是无效gid， 所以如果0处有的话，需要将0的gid全部分过来
	if shards, ok := s2g[0]; ok && len(shards) > 0 {
		return 0
	}
	var keys []int
	for k := range s2g {
		keys = append(keys, k)
	}
	// 按照gid从小到大的顺序进行迭代
	sort.Ints(keys)
	index, max := -1, -1
	for _, gid := range keys {
		if len(s2g[gid]) > max {
			index, max = gid, len(s2g[gid])
		}
	}
	return index
}

// 删除一些raft组，需要移动分片
func (cf *MemoryConfigStateMachine) Leave(gids []int) Err {
	lastConfig := cf.Configs[len(cf.Configs)-1]
	newConfig := Config{len(cf.Configs), lastConfig.Shards, DeepCopy(lastConfig.Groups)}
	s2g := Group2Shards(newConfig)
	// 空余的分片
	orphanShards := make([]int, 0)
	for _, gid := range gids {
		if _, ok := newConfig.Groups[gid]; ok {
			delete(newConfig.Groups, gid)
		}
		if shards, ok := s2g[gid]; ok {
			orphanShards = append(orphanShards, shards...)
			delete(s2g, gid)
		}
	}

	// 重新平衡分片
	var newShards [NShards]int
	// 必须仍存在raft组才能平衡分片
	if len(newConfig.Groups) != 0 {
		for _, shard := range orphanShards {
			// 每次讲分片分给当前最少的raft组
			target := GetGIDWithMinimumShards(s2g)
			s2g[target] = append(s2g[target], shard)
		}
		for gid, shards := range s2g {
			for _, shard := range shards {
				newShards[shard] = gid
			}
		}
	}
	newConfig.Shards = newShards
	cf.Configs = append(cf.Configs, newConfig)
	return OK
}

// 移动分片，但不进行rebalance
func (cf *MemoryConfigStateMachine) Move(shard, gid int) Err {
	lastConfig := cf.Configs[len(cf.Configs)-1]
	newConfig := Config{len(cf.Configs), lastConfig.Shards, DeepCopy(lastConfig.Groups)}
	newConfig.Shards[shard] = gid
	cf.Configs = append(cf.Configs, newConfig)
	return OK
}

// 查询对应的config
func (cf *MemoryConfigStateMachine) Query(num int) (Config, Err) {
	if num == -1 {
		return cf.Configs[len(cf.Configs)-1], OK
	}
	if num >= len(cf.Configs) {
		return *new(Config), NoSuchConfig
	}
	return cf.Configs[num], OK
}

type Op struct {
	// Your data here.
}

func (sc *ShardCtrler) Join(args *CommandRequest, reply *CommandReply) {
	// Your code here.
}

func (sc *ShardCtrler) Leave(args *CommandRequest, reply *CommandReply) {
	// Your code here.
}

func (sc *ShardCtrler) Move(args *CommandRequest, reply *CommandReply) {
	// Your code here.
}

func (sc *ShardCtrler) Query(args *CommandRequest, reply *CommandReply) {

}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func NewCommand(request *CommandRequest) Command {
	command := new(Command)
	command.CommandId = request.CommandId
	command.Op = request.Op
	command.ClientId = request.ClientId
	command.Num = request.Num
	command.Servers = request.Servers
	command.GIDs = request.GIDs
	command.GID = request.GID
	command.Shard = request.Shard

	return *command
}

func (sc *ShardCtrler) Command(request *CommandRequest, reply *CommandReply) {
	defer DPrintf("{Node %v} processes CommandRequest %v with CommandResponse %v", sc.me, request, reply)
	// 保证幂等，避免重复执行，对于写操作不再执行
	//DPrintf("Server %v received the command", sc.me)
	sc.mu.RLock()
	if request.Op != OpQuery && sc.isDuplicateRequest(request.ClientId, request.CommandId) {
		lastReply := sc.lastOperations[request.ClientId].LastReply
		reply.Config, reply.Err = lastReply.Config, lastReply.Err
		sc.mu.RUnlock()
		return
	}
	//DPrintf("here")
	sc.mu.RUnlock()
	//DPrintf("the command is not redudant")
	index, _, isLeader := sc.rf.Start(NewCommand(request))
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	sc.mu.Lock()
	//DPrintf("ready to get NotifyChan")
	ch := sc.getNotifyChan(index)
	//DPrintf("get NotifyChan success")
	sc.mu.Unlock()

	select {
	case result := <-ch:
		reply.Config, reply.Err = result.Config, result.Err
	case <-time.After(ExecueTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		sc.mu.Lock()
		sc.removeOutdatedNotifyChan(index)
		sc.mu.Unlock()
	}()
}

func (sc *ShardCtrler) isDuplicateRequest(clientId, commandId int64) bool {
	lastOperationContext, ok := sc.lastOperations[clientId]
	if ok && lastOperationContext.CommandId == commandId {
		return true
	}
	return false
}

// 移除已经过时的Chan 根据index确定已经完成的chan
func (sc *ShardCtrler) removeOutdatedNotifyChan(index int) {
	delete(sc.notifyChans, index)
}

// 专门用于将已提交目录应用到状态机
func (sc *ShardCtrler) applier() {
	for {
		select {
		case message := <-sc.applyCh:
			//fmt.Printf("{Node %v} tries to apply message %v", kv.me, )
			if message.CommandValid {
				sc.mu.Lock()
				if message.CommandIndex <= sc.lastApplied {
					DPrintf("{Node %v} discards outdated message %v because a newer snapshot which lastApplied is %v has been restored", sc.me, message, sc.lastApplied)
					sc.mu.Unlock()
					continue
				}
				sc.lastApplied = message.CommandIndex

				var reply *CommandReply
				// 之前执行的命令
				command := message.Command.(Command)
				// 重复命令校验
				if command.Op != OpQuery && sc.isDuplicateRequest(command.ClientId, command.CommandId) {
					DPrintf("{Node %v} doesn't apply duplicated message %v to stateMachine because maxAppliedCommandId is %v for client %v", sc.me, message, sc.lastOperations[command.ClientId], command.ClientId)
					reply = sc.lastOperations[command.ClientId].LastReply
				} else {
					reply = sc.applyLogToStateMachine(command)
					// 最后操作表 只需要保存写操作
					if command.Op != OpQuery {
						sc.lastOperations[command.ClientId] = OperationContext{reply, command.CommandId}
					}
				}
				// 应用到server后，需要唤醒之前被阻塞的客户端请求协程
				// 只有在未发生任期切换的情况下才能进行，如果任期已经切换，raft就不能保证这个日志的一致性
				if currentTerm, isLeader := sc.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := sc.getNotifyChan(message.CommandIndex)
					ch <- reply
				}

				//needSnapshot := sc.needSnapshot()
				//if needSnapshot {
				//	sc.takeSnapshot(message.CommandIndex)
				//}
				sc.mu.Unlock()
			} else if message.SnapshotValid {
				// no need to snapshot
				//sc.mu.Lock()
				////if kv.rf.CondInstallSnapShot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
				////if _, isLeader := kv.rf.GetState(); isLeader {
				//fmt.Printf("{Node %v} tries to apply snapshot index %v.\n", kv.me, message.SnapshotIndex)
				////}
				//sc.restoreSnapshot(message.Snapshot, false)
				//sc.lastApplied = message.SnapshotIndex
				////}
				//sc.mu.Unlock()
			} else {
				panic(fmt.Sprintf("unexpected Message %v", message))
			}
		}
	}
}

// 为当前请求封装成Command，被封装index，等待被raft apply
func (sc *ShardCtrler) getNotifyChan(index int) chan *CommandReply {
	// 日志下标序号不可能出现重复
	replyChan, ok := sc.notifyChans[index]
	if ok {
		return replyChan
	}
	sc.notifyChans[index] = make(chan *CommandReply)
	return sc.notifyChans[index]
}

// 将当前命令应用到状态机上
func (sc *ShardCtrler) applyLogToStateMachine(command Command) *CommandReply {
	//defer DPrintf("Node %v applied Command %v to stateMachine.", kv.me, command)

	reply := new(CommandReply)
	if command.Op == OpQuery {
		reply.Config, reply.Err = sc.cf.Query(command.Num)
	} else if command.Op == OpJoin {
		reply.Err = sc.cf.Join(command.Servers)
	} else if command.Op == OpLeave {
		reply.Err = sc.cf.Leave(command.GIDs)
	} else if command.Op == OpMove {
		reply.Err = sc.cf.Move(command.Shard, command.GID)
	} else {
		DPrintf("{Node %v} received an undefined operation %v.", sc.me, command)
	}

	return reply
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Command{})
	labgob.Register(MemoryConfigStateMachine{})

	sc := new(ShardCtrler)
	sc.me = me

	//sc.configs = make([]Config, 1)
	//sc.configs[0].Groups = map[int][]string{}
	sc.cf = NewMemoryConfigStateMachine()

	//labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.lastApplied = 0
	sc.lastOperations = make(map[int64]OperationContext)
	sc.notifyChans = make(map[int]chan *CommandReply)

	// Your code here.
	go sc.applier()

	return sc
}
