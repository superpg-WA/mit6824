package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type OperationContext struct {
	LastReply *CommandReply
	CommandId int64
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	lastApplied  int // 记录当前最后一个被应用的日志，以阻止状态机回滚

	StateMachine   KVStateMachine             // KV状态机
	lastOperations map[int64]OperationContext // 客户端id 2 具体操作，用于判断是否请求是否重复
	notifyChans    map[int]chan *CommandReply // 用于返回请求结果的消息管道
	// Your definitions here.
}

// KV 状态机接口，所有的服务器状态机是相同的。raft对这个一致性进行保证
type KVStateMachine interface {
	Get(key string) (string, Err)
	Put(key, value string) Err
	Append(key, value string) Err

	Log()
}

// KV 状态机的内存实现版本
// 只包装了基本的map操作，无法保证并发可用
type MemoryKV struct {
	KV map[string]string
}

func NewMemoryKV() *MemoryKV {
	return &MemoryKV{make(map[string]string)}
}

func (memoryKV *MemoryKV) Get(key string) (string, Err) {
	if value, ok := memoryKV.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (memoryKV *MemoryKV) Put(key, value string) Err {
	memoryKV.KV[key] = value
	return OK
}

func (memoryKV *MemoryKV) Append(key, value string) Err {
	memoryKV.KV[key] += value
	return OK
}

func (MemoryKV *MemoryKV) Log() {
	for k, v := range MemoryKV.KV {
		fmt.Printf("key:%v.\n value:%v.\n", k, v)
	}
}

func NewCommand(request *CommandRequest) Command {
	command := new(Command)
	command.CommandId = request.CommandId
	command.Key = request.Key
	command.Op = request.Op
	command.Value = request.Value
	command.ClientId = request.ClientId

	return *command
}

func (kv *KVServer) Command(request *CommandRequest, reply *CommandReply) {
	defer DPrintf("{Node %v} processes CommandRequest %v with CommandResponse %v", kv.me, request, reply)
	// 保证幂等，避免重复执行，对于写操作不再执行
	kv.mu.RLock()
	if request.Op != OpGet && kv.isDuplicateRequest(request.ClientId, request.CommandId) {
		lastReply := kv.lastOperations[request.ClientId].LastReply
		reply.Value, reply.Err = lastReply.Value, lastReply.Err
		kv.mu.RUnlock()
		return
	}
	kv.mu.RUnlock()
	index, _, isLeader := kv.rf.Start(NewCommand(request))
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.getNotifyChan(index)
	kv.mu.Unlock()

	select {
	case result := <-ch:
		reply.Value, reply.Err = result.Value, result.Err
	case <-time.After(ExecueTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		kv.removeOutdatedNotifyChan(index)
		kv.mu.Unlock()
	}()
}

// 判断当前请求是否是重复请求
func (kv *KVServer) isDuplicateRequest(clientId, commandId int64) bool {
	lastOperationContext, ok := kv.lastOperations[clientId]
	if ok && lastOperationContext.CommandId == commandId {
		return true
	}
	return false
}

// 为当前请求封装成Command，被封装index，等待被raft apply
func (kv *KVServer) getNotifyChan(index int) chan *CommandReply {
	// 日志下标序号不可能出现重复
	replyChan, ok := kv.notifyChans[index]
	if ok {
		return replyChan
	}
	kv.notifyChans[index] = make(chan *CommandReply)
	return kv.notifyChans[index]
}

// 移除已经过时的Chan 根据index确定已经完成的chan
func (kv *KVServer) removeOutdatedNotifyChan(index int) {
	delete(kv.notifyChans, index)
}

// 专门用于将已提交目录应用到状态机，进行快照并从raft中应用快照的协程
func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		case message := <-kv.applyCh:
			//fmt.Printf("{Node %v} tries to apply message %v", kv.me, )
			if message.CommandValid {
				kv.mu.Lock()
				if message.CommandIndex <= kv.lastApplied {
					DPrintf("{Node %v} discards outdated message %v because a newer snapshot which lastApplied is %v has been restored", kv.me, message, kv.lastApplied)
					kv.mu.Unlock()
					continue
				}
				if message.CommandIndex != kv.lastApplied+1 {
					//fmt.Printf("\n\n unconsecutive log index! message.CommandIndex:%v, kv.lastAppliedIndex:%v. \nLogValue:%v\n", message.CommandIndex, kv.lastApplied, message.Command.(Command))
				}
				if _, isLeader := kv.rf.GetState(); isLeader {
					//fmt.Printf("{Node %v} tries to apply log index %v.\n", kv.me, message.CommandIndex)
				}
				kv.lastApplied = message.CommandIndex

				var reply *CommandReply
				// 之前执行的命令
				command := message.Command.(Command)
				// 重复命令校验
				if command.Op != OpGet && kv.isDuplicateRequest(command.ClientId, command.CommandId) {
					DPrintf("{Node %v} doesn't apply duplicated message %v to stateMachine because maxAppliedCommandId is %v for client %v", kv.me, message, kv.lastOperations[command.ClientId], command.ClientId)
					reply = kv.lastOperations[command.ClientId].LastReply
				} else {
					reply = kv.applyLogToStateMachine(command)
					// 最后操作表 只需要保存写操作
					if command.Op != OpGet {
						kv.lastOperations[command.ClientId] = OperationContext{reply, command.CommandId}
					}
				}
				// 应用到server后，需要唤醒之前被阻塞的客户端请求协程
				// 只有在未发生任期切换的情况下才能进行，如果任期已经切换，raft就不能保证这个日志的一致性
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := kv.getNotifyChan(message.CommandIndex)
					ch <- reply
				}

				// todo: log
				needSnapshot := kv.needSnapshot()
				if needSnapshot {
					kv.takeSnapshot(message.CommandIndex)
				}
				kv.mu.Unlock()
			} else if message.SnapshotValid {
				kv.mu.Lock()
				//if kv.rf.CondInstallSnapShot(message.SnapshotTerm, message.SnapshotIndex, message.Snapshot) {
				// todo: log
				//if _, isLeader := kv.rf.GetState(); isLeader {
				fmt.Printf("{Node %v} tries to apply snapshot index %v.\n", kv.me, message.SnapshotIndex)
				//}
				kv.restoreSnapshot(message.Snapshot, false)
				kv.lastApplied = message.SnapshotIndex
				//}
				kv.mu.Unlock()
			} else {
				panic(fmt.Sprintf("unexpected Message %v", message))
			}
		}
	}
}

// 将当前命令应用到状态机上
func (kv *KVServer) applyLogToStateMachine(command Command) *CommandReply {
	//defer DPrintf("Node %v applied Command %v to stateMachine.", kv.me, command)

	reply := new(CommandReply)
	if command.Op == OpGet {
		reply.Value, reply.Err = kv.StateMachine.Get(command.Key)
	} else if command.Op == OpPut {
		reply.Err = kv.StateMachine.Put(command.Key, command.Value)
	} else if command.Op == OpAppend {
		reply.Err = kv.StateMachine.Append(command.Key, command.Value)
	} else {
		DPrintf("{Node %v} received an undefined operation %v.", kv.me, command)
	}

	return reply
}

// 判断是否需要进行快照
func (kv *KVServer) needSnapshot() bool {
	if kv.maxraftstate < 0 {
		return false
	}
	if kv.rf.ReadPersisterSize() >= kv.maxraftstate {
		return true
	}
	return false
}

// 序列化服务器信息，用于保存快照
// 需要用到状态机信息，最近应用的日志下标（防止状态机回溯）
func (kv *KVServer) encodeStateMachine() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.StateMachine)
	e.Encode(kv.lastOperations)
	return w.Bytes()
}

// 触发raft保存快照
func (kv *KVServer) takeSnapshot(commandIndex int) {
	//fmt.Printf("Node %v take a snapshot, state machine is:\n", kv.me)
	//kv.StateMachine.Log()
	kv.rf.Snapshot(commandIndex, kv.encodeStateMachine())
}

// 根据快照信息重新恢复服务器状态
func (kv *KVServer) restoreSnapshot(snapshot []byte, restart bool) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var stateMachine MemoryKV
	var lastOperations map[int64]OperationContext
	if d.Decode(&stateMachine) != nil ||
		d.Decode(&lastOperations) != nil {
		DPrintf("Server %v decode snapshot Failure!.\n", kv.me)
	} else {
		kv.StateMachine = &stateMachine
		kv.lastOperations = lastOperations
	}
	//if restart {
	//	fmt.Printf("restore Success! ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ kv.stateMachine is:\n")
	//	kv.StateMachine.Log()
	//	fmt.Printf("SnapShot has read is:\n%v\n", len(snapshot))
	//}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Command{})
	labgob.Register(MemoryKV{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.lastApplied = 0
	kv.StateMachine = NewMemoryKV()
	kv.lastOperations = make(map[int64]OperationContext)
	kv.notifyChans = make(map[int]chan *CommandReply)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// 在重启后，从持久层中恢复状态机的状态
	kv.mu.Lock()
	kv.restoreSnapshot(kv.rf.ReadSnapShot(), true)
	kv.lastApplied = kv.rf.SnapshotIndex()
	fmt.Printf("server %v restarts and read state machine from persistence, SnapshotIndex: %v.\n", kv.me, kv.lastApplied)
	kv.mu.Unlock()

	// You may need initialization code here.
	go kv.applier()

	return kv
}
