package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"bytes"
	"fmt"
	"sync/atomic"
	"time"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type ShardKV struct {
	mu      sync.RWMutex
	me      int
	dead    int32
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	makeEnd func(string) *labrpc.ClientEnd
	gid     int
	sc      *shardctrler.Clerk

	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	lastApplied  int // 记录最后应用的日志索引，以阻止状态机回滚

	lastConfig    shardctrler.Config
	currentConfig shardctrler.Config

	stateMachine   map[int]*Shard             // shard KV StateMachine
	lastOperations map[int64]OperationContext // 对于单线程客户端的重复请求去重
	notifyChans    map[int]chan *CommandReply // 请求返回值

	// Your definitions here.
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

type OperationContext struct {
	LastReply *CommandReply
	CommandId int64
}

func (operation OperationContext) DeepCopy() OperationContext {
	ret := new(OperationContext)
	ret.CommandId = operation.CommandId
	// todo: DeepCopy之后也传递指针吗？
	ret.LastReply = operation.LastReply.DeepCopy()
	return *ret
}

// 为当前请求封装成Command，被封装index，等待被raft apply
func (kv *ShardKV) getNotifyChan(index int, mustGetAChan bool) chan *CommandReply {
	// 日志下标序号不可能出现重复
	replyChan, ok := kv.notifyChans[index]
	if ok {
		return replyChan
	}
	if mustGetAChan {
		kv.notifyChans[index] = make(chan *CommandReply, 1)
		return kv.notifyChans[index]
	}
	return nil
}

// GC，移除当前raft组使用过的chan
func (kv *ShardKV) removeOutdatedNotifyChan(index int) {
	delete(kv.notifyChans, index)
}

// 判断是否需要进行快照
// 所有的分片一起进行快照
func (kv *ShardKV) needSnapshot() bool {
	if kv.maxraftstate < 0 {
		return false
	}
	if kv.rf.ReadPersisterSize() >= kv.maxraftstate {
		return true
	}
	return false
}

// todo: 需要序列化分组分片相关信息
// 序列化服务器信息，用于保存快照
// 需要用到状态机信息，最近应用的日志下标（防止状态机回溯）
func (kv *ShardKV) encodeStateMachine() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.stateMachine)
	e.Encode(kv.lastOperations)
	e.Encode(kv.lastConfig)
	e.Encode(kv.currentConfig)
	return w.Bytes()
}

// 触发raft保存快照
func (kv *ShardKV) takeSnapshot(commandIndex int) {
	//fmt.Printf("Node %v take a snapshot, state machine is:\n", kv.me)
	//kv.StateMachine.Log()
	kv.rf.Snapshot(commandIndex, kv.encodeStateMachine())
}

// todo: 需要保存反序列化字段
// 从快照中恢复状态机
// 根据快照信息重新恢复服务器状态
func (kv *ShardKV) restoreSnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var stateMachine map[int]*Shard
	var lastOperations map[int64]OperationContext
	var lastConfig shardctrler.Config
	var currentConfig shardctrler.Config
	if d.Decode(&stateMachine) != nil ||
		d.Decode(&lastOperations) != nil ||
		d.Decode(&lastConfig) != nil ||
		d.Decode(&currentConfig) != nil {
		DPrintf("Server %v decode snapshot Failure!.\n", kv.me)
	} else {
		kv.stateMachine = stateMachine
		kv.lastOperations = lastOperations
		kv.lastConfig = lastConfig
		kv.currentConfig = currentConfig
	}
}

// 当前raft组能否执行该命令
func (kv *ShardKV) canServe(shardId int) bool {
	return kv.currentConfig.Shards[shardId] == kv.gid && (kv.stateMachine[shardId].Status == Serving || kv.stateMachine[shardId].Status == GCing)
}

// 客户端单线程下，判断同一分片范围内请求是否重复
func (kv *ShardKV) isDuplicateRequest(clientId, commandId int64) bool {
	lastOperationContext, ok := kv.lastOperations[clientId]
	if ok && lastOperationContext.CommandId == commandId {
		return true
	}
	return false
}

// 在对应状态机上应用日志
func (kv *ShardKV) applyLogToStateMachine(command *CommandRequest, shardId int) *CommandReply {
	reply := new(CommandReply)
	if command.Op == OpGet {
		reply.Value, reply.Err = kv.stateMachine[shardId].Get(command.Key)
	} else if command.Op == OpPut {
		reply.Err = kv.stateMachine[shardId].Put(command.Key, command.Value)
	} else if command.Op == OpAppend {
		reply.Err = kv.stateMachine[shardId].Append(command.Key, command.Value)
	} else {
		DPrintf("{Node %v} received an undefined operation %v.", kv.me, command)
	}

	return reply
}

// 应用读写命令到状态机
func (kv *ShardKV) applyOperation(message *raft.ApplyMsg, operation *CommandRequest) *CommandReply {
	var reply *CommandReply
	shardId := key2shard(operation.Key)
	// 保证正确性
	if kv.canServe(shardId) {
		// 在应用前需要去重保证正确性
		if operation.Op != OpGet && kv.isDuplicateRequest(operation.ClientId, operation.CommandId) {
			DPrintf("{Node %v}{Group %v} doesn't apply duplicated message %v to stateMachine because maxAppliedCommandId is %v for client %v", kv.me, kv.gid, message, kv.lastOperations[operation.ClientId], operation.ClientId)
			reply = kv.lastOperations[operation.ClientId].LastReply
			return kv.lastOperations[operation.ClientId].LastReply
		} else {
			reply = kv.applyLogToStateMachine(operation, shardId)
			if operation.Op != OpGet {
				kv.lastOperations[operation.ClientId] = OperationContext{reply, operation.CommandId}
			}
			DPrintf("{Node %v}{Group %v} applied command.", kv.me, kv.gid)
			return reply
		}
	}
	return &CommandReply{ErrWrongGroup, ""}
}

func (kv *ShardKV) applyInsertShards(shardsInfo *ShardOperationReply) *CommandReply {
	// 一致性保证，只有配置序号相同才能进行更新
	if shardsInfo.ConfigNum == kv.currentConfig.Num {
		DPrintf("{Node %v}{Group %v} accepts shards insertion %v when currentConfig is %v", kv.me, kv.gid, shardsInfo, kv.currentConfig)
		for shardId, shardData := range shardsInfo.Shards {
			shard := kv.stateMachine[shardId]
			if shard.Status == Pulling {
				for key, value := range shardData {
					shard.KV[key] = value
				}
				shard.Status = GCing
			} else {
				DPrintf("{Node %v}{Group %v} encounters duplicated shards insertion %v when currentConfig is %v", kv.me, kv.gid, shardsInfo, kv.currentConfig)
				break
			}
		}
		for clientId, operationContext := range shardsInfo.LastOperation {
			// 避免状态回退，如果当前raft组处理过对应客户端的服务，需要命令指令不会倒退
			if lastOperation, ok := kv.lastOperations[clientId]; !ok || lastOperation.CommandId < operationContext.CommandId {
				kv.lastOperations[clientId] = operationContext
			}
		}
		return &CommandReply{OK, ""}
	}
	DPrintf("{Node %v}{Group %v} rejects outdated shards insertion %v when currentConfig is %v", kv.me, kv.gid, shardsInfo, kv.currentConfig)
	return &CommandReply{ErrOutDated, ""}
}

func (kv *ShardKV) Command(request *CommandRequest, reply *CommandReply) {
	kv.mu.RLock()
	DPrintf("{Node %v}{Group %v} got a lock for command function.", kv.me, kv.gid)
	// 在收到命令时去重，加快处理速度
	if request.Op != OpGet && kv.isDuplicateRequest(request.ClientId, request.CommandId) {
		DPrintf("{Node %v}{Group %v} doesn't apply duplicated message %v to stateMachine because maxAppliedCommandId is %v for client %v", kv.me, kv.gid, request, kv.lastOperations[request.ClientId], request.ClientId)
		lastReply := kv.lastOperations[request.ClientId].LastReply
		reply.Value, reply.Err = lastReply.Value, lastReply.Err
		kv.mu.RUnlock()
		return
	}
	// 在收到命令时判断，加快处理速度
	if !kv.canServe(key2shard(request.Key)) {
		reply.Err = ErrWrongGroup
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	kv.Execute(NewOperationCommand(request), reply)
}

// 创建信号量而不是直接创建线程，目的：解耦，减少可能的并发问题
func (kv *ShardKV) updateShardStatus(nextConfig *shardctrler.Config) {
	for shard, gid := range nextConfig.Shards {
		// 下一个配置中，当前raft组需要负责这个分片，但是当前配置中，并没有负责该分片
		if gid == kv.gid && kv.currentConfig.Shards[shard] != kv.gid {
			// 如果之前没有负责过这个分片，创建一个新的分片
			if _, ok := kv.stateMachine[shard]; !ok {
				kv.stateMachine[shard] = NewShard()
			}
			// 对于未分配的0分片，不拉取数据
			if kv.currentConfig.Shards[shard] == 0 {
				continue
			}
			kv.stateMachine[shard].Status = Pulling
		}
		// todo: maybe need to update BePulling state
		// 上一个配置中，当前raft组负责该分片，但是下一个配置中不负责了
		if kv.currentConfig.Shards[shard] == kv.gid && gid != kv.gid {
			kv.stateMachine[shard].Status = BePulleding
		}
	}
}

// 应用新配置
func (kv *ShardKV) applyConfiguration(nextConfig *shardctrler.Config) *CommandReply {
	// 应用前校验保证正确性
	if nextConfig.Num == kv.currentConfig.Num+1 {
		DPrintf("{Node %v}{Group %v} updates currentConfig from %v to %v", kv.me, kv.gid, kv.currentConfig, nextConfig)
		kv.updateShardStatus(nextConfig)
		kv.lastConfig = kv.currentConfig
		kv.currentConfig = *nextConfig
		return &CommandReply{OK, ""}
	}
	DPrintf("{Node %v}{Group %v} rejects outdated config %v when currentConfig is %v", kv.me, kv.gid, nextConfig, kv.currentConfig)
	return &CommandReply{ErrOutDated, ""}
}

// 向raft层提交命令、配置更新、数据迁移、垃圾清理等任务
func (kv *ShardKV) Execute(request Command, reply *CommandReply) {
	index, _, isLeader := kv.rf.Start(request)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	DPrintf("{Node %v}{Group %v} got a lock for get a chan.", kv.me, kv.gid)
	ch := kv.getNotifyChan(index, true)
	kv.mu.Unlock()

	select {
	case result := <-ch:
		reply.Value, reply.Err = result.Value, result.Err
		DPrintf("{Node %v}{Group %v} receive a reply from raft.", kv.me, kv.gid)
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeout
		DPrintf("{Node %v}{Group %v} apply timeout from raft.", kv.me, kv.gid)
	}

	go func() {
		kv.mu.Lock()
		DPrintf("{Node %v}{Group %v} got a lock for delete a chan.", kv.me, kv.gid)
		kv.removeOutdatedNotifyChan(index)
		kv.mu.Unlock()
	}()
}

// 在本地服务器中删除对应的分片
func (kv *ShardKV) applyDeleteShards(shardInfos *ShardOperationRequest) *CommandReply {
	if shardInfos.ConfigNum == kv.currentConfig.Num {
		for _, shardId := range shardInfos.ShardIds {
			shard := kv.stateMachine[shardId]
			if shard.Status == GCing {
				shard.Status = Serving
			} else if shard.Status == BePulleding {
				kv.stateMachine[shardId] = NewShard()
			} else {
				DPrintf("{Node %v}{Group %v} encounters duplicated shards deletion %v when currentConfig is %v", kv.me, kv.gid, shardInfos, kv.currentConfig)
				break
			}
		}
		return &CommandReply{OK, ""}
	}
	DPrintf("{Node %v}{Group %v}'s encounters duplicated shards deletion %v when currentConfig is %v", kv.me, kv.gid, shardInfos, kv.currentConfig)
	return &CommandReply{OK, ""}
}

// 应用空日志
func (kv *ShardKV) applyEmptyLog() *CommandReply {
	return &CommandReply{OK, ""}
}

// 接收raft层的日志，执行命令
func (kv *ShardKV) applier() {
	for kv.killed() == false {
		select {
		case message := <-kv.applyCh:
			DPrintf("{Node %v}{Group %v} tries to apply message %v", kv.me, kv.gid, message)
			if message.CommandValid {
				kv.mu.Lock()
				DPrintf("{Node %v}{Group %v} got a lock for apply command.", kv.me, kv.gid)
				if message.CommandIndex <= kv.lastApplied {
					DPrintf("{Node %v}{Group %v} discard the outdated message %v because a newer snapshot which lastApplied is %v has been restored", kv.me, kv.gid, message, kv.lastApplied)
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = message.CommandIndex

				var reply *CommandReply
				command := message.Command.(Command)
				DPrintf("{Node %v}{Group %v} is ready to execute command, op %v.", kv.me, kv.gid, command.Op)
				switch command.Op {
				case Operation:
					operation := command.Data.(CommandRequest)
					reply = kv.applyOperation(&message, &operation)
				case Configuration:
					newConfig := command.Data.(shardctrler.Config)
					reply = kv.applyConfiguration(&newConfig)
				case InsertShards:
					shardsInfo := command.Data.(ShardOperationReply)
					reply = kv.applyInsertShards(&shardsInfo)
				case DeleteShard:
					shardsInfo := command.Data.(ShardOperationRequest)
					reply = kv.applyDeleteShards(&shardsInfo)
				case EmptyEntry:
					reply = kv.applyEmptyLog()
				}

				if currentTerm, isLeader := kv.rf.GetState(); isLeader && currentTerm == message.CommandTerm {
					DPrintf("{Node %v}{Group %v} is still leader.", kv.me, kv.gid)
					ch := kv.getNotifyChan(message.CommandIndex, false)
					DPrintf("{Node %v}{Group %v} got the notifychan", kv.me, kv.gid)
					// 同步通信
					if ch != nil {
						ch <- reply
					}
					DPrintf("{Node %v}{Group %v} applied log to notifychan.", kv.me, kv.gid)
				}

				needSnapshot := kv.needSnapshot()
				if needSnapshot {
					DPrintf("{Node %v}{Group %v} is taking a snapshot.", kv.me, kv.gid)
					kv.takeSnapshot(message.CommandIndex)
				}

				kv.mu.Unlock()
				DPrintf("{Node %v}{Group %v} release a lock for apply command.", kv.me, kv.gid)
			} else if message.SnapshotValid {
				kv.mu.Lock()
				DPrintf("{Node %v}{Group %v} got a lock. for snapshot", kv.me, kv.gid)
				//if kv.rf.CondInstallSnapShot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
				kv.restoreSnapshot(message.Snapshot)
				kv.lastApplied = message.SnapshotIndex
				//}
				kv.mu.Unlock()
			} else {
				panic(fmt.Sprintf("unexpected message %v.", message))
			}
		}
	}
}

// 定期拉取配置，但是如果有分片不处于正常状态，需要阻塞配置的更新
func (kv *ShardKV) configureAction() {
	canPerfomNextConfig := true
	kv.mu.RLock()
	DPrintf("{Node %v}{Group %v} got a lock for configurate.", kv.me, kv.gid)
	for _, shard := range kv.stateMachine {
		if shard.Status != Serving {
			canPerfomNextConfig = false
			DPrintf("{Node %v}{Group %v} will not fetch the latest configuration beacause the status is %v, currentConfig is %v.", kv.me, kv.gid, shard.Status, kv.currentConfig)
			break
		}
	}
	currentConfigNum := kv.currentConfig.Num
	kv.mu.RUnlock()
	DPrintf("{Node %v}{Group %v} release a lock for configure.", kv.me, kv.gid)

	// 不会遗漏任何一个配置，避免不一致。即便剩余多放个配置，会逐一拉取
	if canPerfomNextConfig {
		nextConfig := kv.sc.Query(currentConfigNum + 1)
		if nextConfig.Num == currentConfigNum+1 {
			DPrintf("{Node %v}{Group %v} fetches latest configuration %v when currentConfigNum is %v", kv.me, kv.gid, nextConfig, currentConfigNum)
			kv.Execute(NewConfigurationCommand(&nextConfig), &CommandReply{})
		}
	}
}

// 找到对应状态的分片，用于后续迁移服务
func (kv *ShardKV) getShardIdsByStatus(status ShardStatus) map[int][]int {
	ret := make(map[int][]int)
	for shard, gid := range kv.lastConfig.Shards {
		if shardValue, ok := kv.stateMachine[shard]; ok && shardValue.Status == status {
			ret[gid] = append(ret[gid], shard)
		}
	}
	return ret
}

// 数据迁移 远端服务 处理逻辑
func (kv *ShardKV) GetShardsData(request *ShardOperationRequest, reply *ShardOperationReply) {
	// 减少不一致状态，只从leader处拉取数据
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.RLock()
	DPrintf("{Node %v}{Group %v} got a lock for getdata.", kv.me, kv.gid)
	defer kv.mu.RUnlock()
	defer DPrintf("{Node %v}{Group %v} processes PullTaskRequest %v with reply %v.", kv.me, kv.gid, request, reply)

	// 要求拉取的数据，本raft组还没有更新到，则拒绝服务
	if kv.currentConfig.Num < request.ConfigNum {
		DPrintf("{Node %v}{Group %v} refuse to reply, beacause currentConfigNum is %v, requestConfigNum is %v.", kv.me, kv.gid, kv.currentConfig.Num, request.ConfigNum)
		reply.Err = ErrNotReady
		return
	}

	// 需要同时拷贝切片和去重映射
	reply.Shards = make(map[int]map[string]string)
	for _, shardId := range request.ShardIds {
		// 深拷贝复制数据
		reply.Shards[shardId] = kv.stateMachine[shardId].DeepCopy()
	}

	reply.LastOperation = make(map[int64]OperationContext)
	for clientId, operation := range kv.lastOperations {
		reply.LastOperation[clientId] = operation.DeepCopy()
	}

	reply.ConfigNum, reply.Err = request.ConfigNum, OK
}

// 数据迁移句柄
func (kv *ShardKV) migrationAction() {
	DPrintf("{Node %v}{Group %v} starts a migrationAction, without a lock.", kv.me, kv.gid)
	kv.mu.RLock()
	DPrintf("{Node %v}{Group %v} got a lock for migration.", kv.me, kv.gid)
	// 查询当前状态为Pulling的分片，更新这些分片内容
	DPrintf("{Node %v}{Group %v} starts a migrationAction. After a lock.", kv.me, kv.gid)
	gid2shardIds := kv.getShardIdsByStatus(Pulling)
	DPrintf("{Node %v}{Group %v} starts a migrationAction, gid2shardIds is %v.", kv.me, kv.gid, gid2shardIds)
	// 不能出现中间状态，加锁保证配置更新过程中的原子性
	var wg sync.WaitGroup
	for gid, shardIds := range gid2shardIds {
		DPrintf("{Node %v}{Group %v} starts a PullTask to get shards %v from group %v when config is %v.", kv.me, kv.gid, shardIds, gid, kv.currentConfig)
		wg.Add(1)
		// 从对应gid中拉取需要迁移的分片数据
		go func(servers []string, configNum int, shardIds []int) {
			defer wg.Done()
			pullTaskRequest := ShardOperationRequest{configNum, shardIds}
			// 从对应集群拉取对应的分片数据
			for _, server := range servers {
				var pullTaskReply ShardOperationReply
				srv := kv.makeEnd(server)
				// 拉取数据 和 raft日志是异步的
				if srv.Call("ShardKV.GetShardsData", &pullTaskRequest, &pullTaskReply) && pullTaskReply.Err == OK {
					DPrintf("{Node %v}{Group %v} gets a PullTaskReply %v and tries to commit it when currentConfigNum is %v", kv.me, kv.gid, pullTaskReply, configNum)
					// 拉取到数据后，应用新的分片需要在日志中定序
					kv.Execute(NewInsertShardsCommand(&pullTaskReply), &CommandReply{})
				} else {
					DPrintf("{Node %v}{Group %v} failt to get a PullTaskReply, because %v.", kv.me, kv.gid, pullTaskReply.Err)
				}
			}
		}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, shardIds)
	}
	kv.mu.RUnlock()
	wg.Wait()
}

// 数据删除 远端服务 处理逻辑
func (kv *ShardKV) DeleteShardsData(request *ShardOperationRequest, reply *ShardOperationReply) {
	// 减少不一致，只由leader处理分片数据删除
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	defer DPrintf("{Node %v}{Group %v} processes GCTaskRequest %v with reply %v.", kv.me, kv.gid, request, reply)

	kv.mu.RLock()
	DPrintf("{Node %v}{Group %v} got a lock for delete.", kv.me, kv.gid)
	// 只回收当前配置号下的分片
	if kv.currentConfig.Num > request.ConfigNum {
		DPrintf("{Node %v}{Group %v}'s encounters duplicated shards deletion %v when currentConfig is %v.", kv.me, kv.gid, request.ShardIds, kv.currentConfig.Num)
		// 过时的请求不需要再被处理了，返回OK
		reply.Err = OK
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	DPrintf("{Node %v}{Group %v} releas a lock for delete.", kv.me, kv.gid)

	var commandReply CommandReply
	// 和其余句柄不同，远端服务需要执行删除命令，并在日志中定序
	kv.Execute(NewDeleteShardsCommand(request), &commandReply)

	reply.Err = commandReply.Err
}

// GC句柄
func (kv *ShardKV) gcAction() {
	kv.mu.RLock()
	DPrintf("{Node %v}{Group %v} got a lock for gc.", kv.me, kv.gid)
	gid2ShardIds := kv.getShardIdsByStatus(GCing)
	var wg sync.WaitGroup
	for gid, shardIds := range gid2ShardIds {
		DPrintf("{Node %v}{Group %v} starts a GCTask to delete shards %v in group %v when config is %v.", kv.me, kv.gid, shardIds, gid, kv.currentConfig)
		wg.Add(1)
		go func(servers []string, configNum int, shardIds []int) {
			defer wg.Done()
			gcTaskRequest := ShardOperationRequest{configNum, shardIds}
			for _, server := range servers {
				var gcTaskReply ShardOperationReply
				srv := kv.makeEnd(server)
				if srv.Call("ShardKV.DeleteShardsData", &gcTaskRequest, &gcTaskReply) && gcTaskReply.Err == OK {
					DPrintf("{Node %v}{Group %v} deletes shards %v in remote group successfully when currentConfigNum is %v.", kv.rf, kv.gid, shardIds, configNum)
					kv.Execute(NewDeleteShardsCommand(&gcTaskRequest), &CommandReply{})
				}
			}
		}(kv.lastConfig.Groups[gid], kv.currentConfig.Num, shardIds)
	}
	kv.mu.RUnlock()
	wg.Wait()
}

// 应用空日志
func (kv *ShardKV) checkEntryInCurrentTermAction() {
	if !kv.rf.HasLogInCurrentTerm() {
		kv.Execute(NewEmptyCommand(), &CommandReply{})
	}
}

// 监控器，定时执行命令
func (kv *ShardKV) Monitor(action func(), timeout time.Duration) {
	for kv.killed() == false {
		//DPrintf("{Node %v}{Group %v} starts a action.", kv.me, kv.gid)
		if _, isLeader := kv.rf.GetState(); isLeader {
			action()
		}
		time.Sleep(timeout)
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	// todo: register type which need to be stored
	labgob.Register(Command{})
	labgob.Register(CommandRequest{})
	labgob.Register(CommandReply{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(ShardOperationRequest{})
	labgob.Register(ShardOperationReply{})

	applyCh := make(chan raft.ApplyMsg)

	kv := &ShardKV{
		me:      me,
		dead:    0,
		rf:      raft.Make(servers, me, persister, applyCh),
		applyCh: applyCh,

		makeEnd: make_end,
		gid:     gid,
		sc:      shardctrler.MakeClerk(ctrlers),

		ctrlers:      ctrlers,
		maxraftstate: maxraftstate,
		lastApplied:  0,

		lastConfig:    shardctrler.DefaultConfig(),
		currentConfig: shardctrler.DefaultConfig(),

		stateMachine:   make(map[int]*Shard),
		lastOperations: make(map[int64]OperationContext),
		notifyChans:    make(map[int]chan *CommandReply),
	}

	kv.restoreSnapshot(kv.rf.ReadSnapShot())
	kv.lastApplied = kv.rf.SnapshotIndex()

	//kv := new(ShardKV)
	//kv.me = me
	//kv.maxraftstate = maxraftstate
	//kv.makeEnd = make_end
	//kv.gid = gid
	//kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	//kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// start applier goroutine to apply committed logs to statemachine
	go kv.applier()
	// start configuration monitor to fetch the latest configuration
	go kv.Monitor(kv.configureAction, ConfigureMonitorTimeout)
	// start migration monitor goroutine to pull related shards
	go kv.Monitor(kv.migrationAction, MigrationMonitorTimeout)
	// start gc monitor goroutine to delete useless shards in remote groups
	go kv.Monitor(kv.gcAction, GCMonitorTimeout)
	// start entry-in-currentTerm monitor goroutine to advance commitIndex by appending empty entries in current term periodically to avoid live locks
	go kv.Monitor(kv.checkEntryInCurrentTermAction, EmptyEntryDetectorTimeout)

	DPrintf("{Node %v}{Group %v} has started", kv.me, kv.gid)
	return kv
}
