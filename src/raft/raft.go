package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

/*
Make接口：创建一个Raft服务器
Start接口：将命令追加到log日志中，并且需要立即返回。也就是说追加这个操作是异步的，
GetState接口：询问raft当前的term，以及它是否认为自己是leader
ApplyMsg：将命令追加到日志中，每个raft节点应该向服务发送ApplyMsg
*/

import (
	"6.5840/labgob"
	"bytes"
	"fmt"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// 日志条目 类
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 2A
	currentTerm int // 当前的任期 需要持久化
	votedFor    int // 当前节点在当前任期投票给了谁，默认没有投为-1
	state       int // 当前节点自己的状态
	//voteReceived   int         // 当前term中，当前节点收到的选票数目
	//lastHeartBeat  time.Time   // 最后一次心跳的时间 不需要，被计时器替换掉了
	heartBeatTimer *time.Timer // 心跳计时器
	electionTimer  *time.Timer // 选举计时器

	// 2B
	log         []LogEntry // 日志条目 下标从1开始
	commitIndex int        // 已提交到所有服务器的状态机的日志条目的索引号
	lastApplied int        // 已提交到本地状态机的日志条目的索引号

	applyCh        chan ApplyMsg // commited后，需要将command发送到应用层
	applyCond      *sync.Cond    // 提交日志条目后，唤醒applier线程
	replicatorCond []*sync.Cond  // 唤醒 replicator 协程用于复制日志

	// leader state
	nextIndex  []int // Leader 下一次 向 Follower发送日志时，要发送的第一个日志条目的索引号 为每个服务器都维护一个变量 从最后一个开始发送
	matchIndex []int // Follower 已复制到本地日志中，并且已经应用到状态机的日志条目的最大索引号

	//logIndex int // 日志索引变量，用于Leader生成唯一的索引
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.state == LEADER {
		isleader = true
	} else {
		isleader = false
	}
	return term, isleader
}

// Raft 的持久化状态
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	raftstate := rf.encodeState()
	rf.persister.SaveRaftState(raftstate)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var logs []LogEntry
	var currentTerm int
	var votedFor int
	if d.Decode(&logs) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil {
		fmt.Printf("Raft decode persist state Failure!.\n")
	} else {
		rf.log = logs
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
	}
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.log)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	return w.Bytes()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// follower 可以在 leader不知情的情况下创建快照

	snapshotIndex := rf.getFirstLog().Index
	if index <= snapshotIndex {
		fmt.Printf("ID %v refuse snapshot, because index smaller than snapshotIndex. index %v, snapshotIndex %v.\n", rf.me, index, snapshotIndex)
		return
	}
	rf.log = shrinkEntriesArray(rf.log[index-snapshotIndex:])
	//fmt.Printf("After snapshot ID %v log first index is %v.\n", rf.me, rf.getFirstLog().Index)
	rf.log[0].Command = nil
	rf.persister.Save(rf.encodeState(), snapshot)
	//fmt.Printf("Node %v snapshot. state is %v, term %v, commitIndex %v, lastApplied %v.\n", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied)
	//snapShotRequest := new(InstallSnapsShotRequest)
	//snapShotRequest.Term = rf.currentTerm
	//snapShotRequest.LeaderId = rf.me
	//snapShotRequest.LastIncludedIndex = index
	//snapShotRequest.LastIncludedTerm = rf.log[index-rf.getFirstLog().Index].Term

}

type InstallSnapsShotRequest struct {
	Term              int    // leader任期
	LeaderId          int    // leader ID
	LastIncludedIndex int    // ? 快照取代所有的日志条目，包括这个index以及之前所有的日志
	LastIncludedTerm  int    // LastIncludedIndex的任期
	Offset            int    // 在快照文件中，chunk所在位置的字节偏移量
	Data              []byte // 快照的原始字节，从offset开始
	Done              bool   // 如果这是最后的块，返回true
}

type InstallSnapsShotReply struct {
	Term int // follower的term，用于leader更新自己
}

func (rf *Raft) InstallSnapShot(args *InstallSnapsShotRequest, reply *InstallSnapsShotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//fmt.Printf("ID %v receive snapShot Rpc from %v. lastIncludeIndex: %v, lastIncludeTerm: %v.\n", rf.me, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)

	reply.Term = rf.currentTerm

	// leader任期更小，忽略
	if args.Term < rf.currentTerm {
		//fmt.Printf("ID %v refuse snapshot RPC beacuase currentTerm %v, leader term %v.\n", rf.me, rf.currentTerm, args.Term)
		return
	}

	// 自己的任期更小，更新任期信息（因为持久化的要求，每次更新后都需要持久化）
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}

	rf.state = FOLLOWER
	rf.electionTimer.Reset(RandomElectionTimeout())

	// 如果快照的下标小于已经提交的下标，快照的信息已经提交，没有必要再同步这个快照了
	if args.LastIncludedIndex <= rf.commitIndex {
		//fmt.Printf("ID %v refuse snapShot RPC beacause the index %v has been commited %v.\n", rf.me, args.LastIncludedIndex, rf.commitIndex)
		return
	}

	//rf.CondInstallSnapShot(args.LastIncludedTerm, args.LastIncludedIndex, args.Data)
	if args.LastIncludedIndex > rf.getLastLog().Index {
		rf.log = make([]LogEntry, 1)
	} else {
		// 快照的内容在日志数组里，缩短日志数组，避免内存泄露
		rf.log = shrinkEntriesArray(rf.log[args.LastIncludedIndex-rf.getFirstLog().Index:])
		rf.log[0].Command = nil
	}

	rf.log[0].Term, rf.log[0].Index = args.LastIncludedTerm, args.LastIncludedIndex
	// 不需要传送落后的日志，应用层可以通过快照获取这些信息
	rf.lastApplied, rf.commitIndex = args.LastIncludedIndex, args.LastIncludedIndex

	// 快照和状态更新后需要持久化
	rf.persister.Save(rf.encodeState(), args.Data)

	//fmt.Printf("ID %v accept snapShot RPC from leader %v.\n", rf.me, args.LeaderId)
	// 快照数据是从应用层来的，必须发送到follower对应的应用从处理
	go func() {
		rf.applyCh <- ApplyMsg{
			Command:       false,
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()

}

func (rf *Raft) sendInstallSnapShot(server int, args *InstallSnapsShotRequest, reply *InstallSnapsShotReply) bool {
	//fmt.Printf("Leader %v call %v snapShot.\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}

// 进行数值传递，防止内存泄露
func shrinkEntriesArray(array []LogEntry) []LogEntry {
	return append([]LogEntry{}, array...)
}

// 将来服务层触发的快照
func (rf *Raft) CondInstallSnapShot(lastIncludeTerm int, lastIncludeIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果快照信息都已经提交，没必要触发快照
	if lastIncludeIndex <= rf.commitIndex {
		return false
	}

	// 如果需要快照的内容不够，重置日志数组
	if lastIncludeIndex > rf.getLastLog().Index {
		rf.log = make([]LogEntry, 1)
	} else {
		// 快照的内容在日志数组里，缩短日志数组，避免内存泄露
		rf.log = shrinkEntriesArray(rf.log[lastIncludeIndex-rf.getFirstLog().Index:])
		rf.log[0].Command = nil
	}

	rf.log[0].Term, rf.log[0].Index = lastIncludeTerm, lastIncludeIndex
	rf.lastApplied, rf.commitIndex = lastIncludeIndex, lastIncludeIndex

	// 快照和状态更新后需要持久化
	rf.persister.Save(rf.encodeState(), snapshot)
	return true
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's log entry
	LastLogTerm  int // term of candidate's last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term      int  // currentTerm, for candidate to update itself
	VoteGrand bool // true means candidate received vote
}

// example RequestVote RPC handler.
// 收到RequestVote后的处理方法
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 1. 检查Term是否大于自己，如果大于，放弃选举，给候选人投票，更新term
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 函数完全调用完成后，才发起persist持久化
	defer rf.persist()
	//defer fmt.Printf("ID: %v receive RequestVote. serverTerm: %v, argsTerm: %v, VoteFor: %v, VoteGrant.: %v.\n", rf.me, rf.currentTerm, args.Term, rf.votedFor, reply.VoteGrand)

	//fmt.Printf("ID %v term %v receive vote request from %v term %v.\n", rf.me, rf.currentTerm, args.CandidateId, args.Term)

	// 不投票的情况
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		//fmt.Printf("ID: %v receive RequestVote RPC, Refuse vote. args.Term: %v, rf.currentTerm: %v, rf.voteFor: %v, candidateId: %v.\n", rf.me, args.Term, rf.currentTerm, rf.votedFor, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGrand = false
		//fmt.Printf("ID %v refuse to vote because args.Term %v, rf.voteFor %v.\n", rf.me, args.Term, rf.votedFor)
		return
	}

	// 任期没有candidate高，自己退为follower
	if args.Term > rf.currentTerm {
		//fmt.Printf("ID: %v term %v behind candidate term %v.\n", rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		// 更换term后，清空投票内容，但是这一轮要投票给candidate
		rf.votedFor = -1
		//fmt.Printf("ID: %v term changed to %v. Because of Receive RequestVote RPC.\n", rf.me, rf.currentTerm)
		rf.state = FOLLOWER
	}

	// 选举限制
	if !rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.Term, reply.VoteGrand = rf.currentTerm, false
		//fmt.Printf("candidate %v dont meet election restriction, %v refuse vote. args.LastLogTerm %v, args.LastLogIndex %v, rf.lastTerm %v, rf.lastIndex %v.\n", args.CandidateId, rf.me, args.LastLogTerm, args.LastLogIndex, rf.getLastLog().Term, rf.getLastLog().Index)
		return
	}

	//fmt.Printf("run here. args.CandidateId: %v.\n", args.CandidateId)
	rf.votedFor = args.CandidateId
	// 重置选举时间
	rf.electionTimer.Reset(RandomElectionTimeout())
	reply.Term = rf.currentTerm
	reply.VoteGrand = true
	//fmt.Printf("votedFor: %v.\n", rf.votedFor)
}

/*
选举限制
1. 候选人最后一条Log条目的任期号大于本地最后一条Log条目的任期号
2. 候选人最后一条Log条目的任期号等于本地最后一条Log条目的任期号，且候选人的Log记录长度大于等于本地Log记录程度
*/
func (rf *Raft) isLogUpToDate(lastLogTerm, lastLogIndex int) bool {
	if lastLogTerm > rf.getLastLog().Term || (lastLogTerm == rf.getLastLog().Term && lastLogIndex >= rf.getLastLog().Index) {
		return true
	}
	return false
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//fmt.Printf("voteGranted: %v.\n", reply.VoteGrand)
	//// 如果发别其他节点的term更高，自己降为follower，更新term
	//rf.mu.Lock()
	//if rf.currentTerm < reply.Term {
	//
	//	rf.state = FOLLOWER
	//	rf.currentTerm = reply.Term
	//	//fmt.Printf("ID: %v term changed to %v. Because of sendRequestVote RPC.\n", rf.me, rf.currentTerm)
	//
	//} else {
	//	// 增加当前的选票
	//	if reply.VoteGrand == 1 && rf.state != LEADER {
	//		rf.voteReceived++
	//		n := len(rf.peers)
	//		if rf.voteReceived > n/2 {
	//			rf.state = LEADER
	//			//fmt.Printf("ID: %v is new Leader. Its term is %v.\n", rf.me, rf.currentTerm)
	//		}
	//
	//	}
	//}
	//rf.mu.Unlock()

	return ok
}

type AppendEntriesRequest struct {
	Term         int        // leader's term
	LeaderId     int        // index of leader
	PrevLogIndex int        // leader已知的最后一条日志条目的索引
	PrevLogTerm  int        // leader已知的最后一条日志条目的任期号
	Entries      []LogEntry // 存储日志条目 心跳时为空，可能因为效率发送多次
	LeaderCommit int        // leader的提交下标
}

type AppendEntriesReply struct {
	Term          int  // terms of other servers
	Success       bool // true if follower contained entry matching prevLogIndex and prevLogTerm
	ConflictTerm  int  // 出现冲突的任期 用于快速恢复
	ConflictIndex int  // 出现冲突的索引 用于快速恢复
	//ConflictLen   int  //
}

func (rf *Raft) matchLog(prevLogTerm, prevLogIndex int) bool {
	if len(rf.log) > prevLogIndex-rf.getFirstLog().Index {
		return rf.log[prevLogIndex-rf.getFirstLog().Index].Term == prevLogTerm
	} else {
		return false
	}
}

// 更新 Follower 的commitIndex参数
func (rf *Raft) advanceCommitIndexForFollower(leaderCommit int) {
	if IntegerMin(leaderCommit, rf.getLastLog().Index) > rf.commitIndex {
		rf.commitIndex = IntegerMin(leaderCommit, rf.getLastLog().Index)
		rf.applyCond.Signal()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesReply) {
	//fmt.Printf("ID: %v receive AppendEntries. leaderID: %v, leaderTerm: %v. args.PrevLogIndex %v, rf.firstLogIndex %v, log length %v.\n", rf.me, args.LeaderId, args.Term, args.PrevLogIndex, rf.getFirstLog().Index, len(rf.log))
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// 参数的任期更小，不match
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		//fmt.Printf("ID: %v term is %v bigger than leader term %v.\n", rf.me, rf.currentTerm, args.Term)
		return
	}

	// 参数的任期更大，自己退回Follower
	if args.Term > rf.currentTerm {
		//fmt.Printf("ID: %v term %v behind args term %v.\n", rf.me, rf.currentTerm, args.Term)
		rf.currentTerm = args.Term
		// 更新term的同时清空 votedFor
		rf.votedFor = -1
	}

	// 收到的任期大于等于自己，就会被重置为Follower
	rf.state = FOLLOWER
	rf.electionTimer.Reset(RandomElectionTimeout())
	//fmt.Printf("ID: %v receive heartBeat, reset electionTimeout.\n", rf.me)

	// 异常情况，日志索引不在当前服务器的记录中
	if args.PrevLogIndex < rf.getFirstLog().Index {
		//fmt.Printf("ID %v return AppendEntries RPC false. because args.PrevLogIndex %v < rf.getFirstLogIndex %v.\n", rf.me, args.PrevLogIndex, rf.getFirstLog().Index)
		reply.Term, reply.Success = 0, false
		return
	}

	// 如果prevLogIndex处日志的任期不匹配
	if !rf.matchLog(args.PrevLogTerm, args.PrevLogIndex) {
		reply.Term, reply.Success = rf.currentTerm, false
		// 优化：加速日志冲突恢复
		lastIndex := rf.getLastLog().Index
		if lastIndex < args.PrevLogIndex {
			// 日志缺失： follower日志没有leader长
			reply.ConflictTerm = -1
			reply.ConflictIndex = lastIndex + 1
		} else {
			// 找到第一个冲突的日志，并删除该日志后面的所有日志
			// 半数commit原则保证了这里的删除正确性
			firstIndex := rf.getFirstLog().Index
			reply.ConflictTerm = rf.log[args.PrevLogIndex-firstIndex].Term
			index := args.PrevLogIndex - 1
			// 返回peer的日志中，任期等于XTerm的第一个日志
			for index >= firstIndex && rf.log[index-firstIndex].Term == reply.ConflictTerm {
				index--
			}
			reply.ConflictIndex = index
		}
		return
	}

	// 如果接收一个appendEntries消息，那么需要首先删除本地相应的Log，再用AppendEntries中的内容替代本地Log。
	firstIndex := rf.getFirstLog().Index
	for index, entry := range args.Entries {
		if entry.Index-firstIndex >= len(rf.log) || rf.log[entry.Index-firstIndex].Term != entry.Term {
			//rf.log = append(rf.log[:entry.Index-firstIndex], args.Entries[index:]...)
			// todo: shrink，日志压缩 防止内存泄露
			rf.log = shrinkEntriesArray(append(rf.log[:entry.Index-firstIndex], args.Entries[index:]...))
			break
		}
	}

	// 如果leaderCommit > commitIndex 设置commitIndex为min(leaderCommit, index of last new entry)
	rf.advanceCommitIndexForFollower(args.LeaderCommit)

	//fmt.Printf("ID: %v receive AppendEntries Request, no conflict. commitIndex: %v.\n", rf.me, rf.commitIndex)

	reply.Term, reply.Success = rf.currentTerm, true

	//if args.Term == rf.currentTerm {
	//	// 检验在 prevLogIndex 的日志是否为 prevLogTerm
	//	if rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
	//
	//	} else {
	//
	//	}
	//	if rf.state == FOLLOWER {
	//
	//		rf.lastHeartBeat = time.Now()
	//		reply.Term = rf.currentTerm
	//
	//	} else if rf.state == CANDIDATE {
	//		// leader 的 term大于等于自己，竞选失败
	//
	//		rf.state = FOLLOWER
	//		rf.lastHeartBeat = time.Now()
	//		reply.Term = rf.currentTerm
	//
	//	} else {
	//		// todo: 考虑 log
	//		// 相等term统一处理成退回
	//		if rf.currentTerm == args.Term {
	//			//rf.mu.Lock()
	//			//rf.state = FOLLOWER
	//			//rf.currentTerm = args.Term
	//			//rf.mu.Unlock()
	//		}
	//		reply.Term = rf.currentTerm
	//	}
	//}

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesReply) bool {
	//fmt.Printf("Leader %v send AppendEntries to %v. entry log index from %v to %v.\n", rf.me, server, args.PrevLogIndex, args.PrevLogIndex+len(args.Entries)-1)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//// 如果发别其他节点的term更高，自己降为follower，更新term
	//rf.mu.Lock()
	//if rf.currentTerm < reply.Term {
	//
	//	rf.state = FOLLOWER
	//	rf.currentTerm = reply.Term
	//	//fmt.Printf("ID: %v term changed to %v. Because of Send AppendLog RPC.\n", rf.me, rf.currentTerm)
	//
	//	//fmt.Printf("ID: %v, term: %v was leader but found %v term higher.\n", rf.me, rf.currentTerm, reply.Term)
	//}
	//rf.mu.Unlock()
	return ok
}

// 由leader调用，追加新的command并包装为Entry
func (rf *Raft) appendNewLogEntry(command interface{}) LogEntry {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	//defer rf.persist()

	rf.log = append(rf.log, LogEntry{
		Index:   rf.getLastLog().Index + 1,
		Term:    rf.currentTerm,
		Command: command,
	})

	//fmt.Printf("Append Function result: %v.\n", rf.log)

	return rf.getLastLog()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
/*
	入参：命令 command
	出参：命令提交后的下标 int, 任期term int, 节点是否是leader bool
*/
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//fmt.Printf("{Node %v} (state %v) receives a new command[%v] to replicate in term %v.\n", rf.me, rf.state, command, rf.currentTerm)
	// 先判断自己失败不是leader，如果不是，则直接放弃执行命令
	if rf.state != LEADER {
		return -1, -1, false
	}
	//fmt.Printf("Leader: %v receive Start Function.\n", rf.me)
	newLog := rf.appendNewLogEntry(command)
	//fmt.Printf("{Node %v} receives a new command to replicate in term %v.\n", rf.me, rf.currentTerm)
	// 应用层调用 start 函数后，leader向其他节点发送日志
	rf.leaderHeartBeat(false)
	return newLog.Index, newLog.Term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) genInstallSnapshotRequest() *InstallSnapsShotRequest {
	request := new(InstallSnapsShotRequest)
	request.Term = rf.currentTerm
	request.LeaderId = rf.me
	request.LastIncludedIndex = rf.getFirstLog().Index
	request.LastIncludedTerm = rf.getFirstLog().Term
	request.Data = rf.persister.ReadSnapshot()
	// 不需要 done、 offeset
	return request
}

func (rf *Raft) handleInstallSnapshotReply(peer int, request *InstallSnapsShotRequest, reply *InstallSnapsShotReply) {
	//fmt.Printf("Leader %v send snapShot RPC to %v success. lastIncludeIndex: %v.\n", rf.me, peer, request.LastIncludedIndex)

	if reply.Term > rf.currentTerm {
		rf.state = FOLLOWER
		rf.electionTimer.Reset(RandomElectionTimeout())
		rf.votedFor = -1
		rf.currentTerm = reply.Term
		return
	}

	rf.nextIndex[peer] = request.LastIncludedIndex + 1
	rf.matchIndex[peer] = request.LastIncludedIndex
}

func (rf *Raft) replicateOneRound(peer int) {
	rf.mu.RLock()
	// double check
	if rf.state != LEADER {
		rf.mu.RUnlock()
		return
	}

	prevLogIndex := rf.nextIndex[peer] - 1
	//fmt.Printf("Leader %v replicate one round to %v. prevLogIndex: %v, rf.getFirstLog().Index: %v.\n", rf.me, peer, prevLogIndex, rf.getFirstLog().Index)
	if prevLogIndex < rf.getFirstLog().Index {
		//fmt.Printf("Leader %v send snapshotPRC to %v.\n", rf.me, peer)
		// 日志压缩才能赶上进度
		request := rf.genInstallSnapshotRequest()
		//fmt.Printf("Leader %v genInstallSnapShotRequest Success.\n", rf.me)
		rf.mu.RUnlock()
		reply := new(InstallSnapsShotReply)
		//fmt.Printf("Leader %v before call sendInstallSnapShot.\n", rf.me)
		if rf.sendInstallSnapShot(peer, request, reply) {
			rf.mu.Lock()
			rf.handleInstallSnapshotReply(peer, request, reply)
			rf.mu.Unlock()
		} else {
			//fmt.Printf("Leader %v send snapShot RPC to %v false.\n", rf.me, peer)
		}
	} else {
		// 发送日志entry就可以刚拿上进度
		// send AppendEntries RPC
		request := rf.genAppendEntriesRequest(prevLogIndex)
		//fmt.Printf("Leader %v send AppendEntries RPC to %v. PrevLogIndex %v.\n", rf.me, peer, request.PrevLogIndex)
		//request := new(AppendEntriesRequest)
		//request.Term = rf.currentTerm
		//request.LeaderId = rf.me
		//request.Entries = for()
		rf.mu.RUnlock()
		reply := new(AppendEntriesReply)
		//rf.persist()
		if rf.sendAppendEntries(peer, request, reply) {
			rf.mu.Lock()
			rf.handleAppendEntriesReply(peer, request, reply)
			rf.mu.Unlock()
		} else {
			//fmt.Printf("Leader %v send AppendEntries to %v false.\n", rf.me, peer)
		}
	}

}

func (rf *Raft) genAppendEntriesRequest(prevLogIndex int) *AppendEntriesRequest {
	request := new(AppendEntriesRequest)
	request.Term = rf.currentTerm
	request.LeaderId = rf.me
	// 发送的日志条目从 nextIndex 到结尾
	request.Entries = rf.log[prevLogIndex-rf.getFirstLog().Index+1:]
	//if len(rf.log) > prevLogIndex+1 {
	//	request.Entries = rf.log[prevLogIndex+1:]
	//} else {
	//	request.Entries = make([]LogEntry, 0)
	//}
	request.PrevLogIndex = prevLogIndex
	request.PrevLogTerm = rf.log[prevLogIndex-rf.getFirstLog().Index].Term
	// todo: 检查leadercommit的含义
	request.LeaderCommit = rf.commitIndex

	return request
}

// 处理收到reply后的处理过程
func (rf *Raft) handleAppendEntriesReply(peer int, request *AppendEntriesRequest, reply *AppendEntriesReply) {

	// 当前的任期比Follower的任期低，退回Follower
	if rf.currentTerm < reply.Term {
		rf.currentTerm = reply.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist()
		//fmt.Printf("leader %v term is %v smaller than peer %v term %v, become follower.\n", rf.me, rf.currentTerm, peer, reply.Term)
		return
	}

	// 如果
	if len(request.Entries) == 0 {
		//fmt.Printf("Leader: %v receive heartBeat response from %v.\n", rf.me, peer)
		return
	}

	//fmt.Printf("Leader: %v receive AppendEntries respone. reply.Success: %v. request log index from %v to %v.\n", rf.me, reply.Success, request.PrevLogIndex, request.PrevLogIndex+len(request.Entries)-1)
	// 日志冲突，重新发送 AppendEntries RPC
	if reply.Success == false {
		// 这里应该修改 nextIndex的值，因为没有catch，会一直对peer的AppendEntries过程。
		if reply.ConflictTerm == -1 {
			rf.nextIndex[peer] = reply.ConflictIndex
		} else {
			nextIndex := reply.ConflictIndex
			// 过于靠后的消息，无法通过log完成了，等下一次的log同步
			if nextIndex < rf.getFirstLog().Index || reply.ConflictTerm < rf.currentTerm {
				return
			}
			for ; nextIndex < rf.getFirstLog().Index+len(rf.log); nextIndex++ {
				if rf.log[nextIndex-rf.getFirstLog().Index].Term != reply.ConflictTerm {
					break
				}
			}
			rf.nextIndex[peer] = nextIndex
		}
		//newRequest := new(AppendEntriesRequest)
		//newRequest.Term = rf.currentTerm
		//newRequest.LeaderId = rf.me
		//newRequest.LeaderCommit = rf.commitIndex
		//prevIndex := reply.ConflictIndex
		//for ; prevIndex <= len(rf.log); prevIndex++ {
		//	if rf.log[prevIndex].Term != reply.ConflictTerm {
		//		break
		//	}
		//}
		////newRequest.PrevLogIndex = prevIndex
		////newRequest.PrevLogTerm = rf.log[prevIndex].Term
		////newRequest.Entries = rf.log[prevIndex:]
		//
		//reply := new(AppendEntriesReply)
		//newRequest := rf.genAppendEntriesRequest(prevIndex)
		//if rf.sendAppendEntries(peer, newRequest, reply) {
		//	rf.handleAppendEntriesReply(peer, newRequest, reply)
		//}
	} else {
		// 维护 nextIndex 值
		// 因为这次发送的日志都被完整拷贝了，所以为最后一个日志的index + 1
		rf.nextIndex[peer] = request.Entries[len(request.Entries)-1].Index + 1
		rf.matchIndex[peer] = IntegerMax(rf.matchIndex[peer], request.Entries[len(request.Entries)-1].Index)
		// todo: 校验是否半数以上都达到了 matchIndex， 从而更新 commitIndex
		matchIndex := rf.matchIndex[peer]
		cnt := 0
		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			if rf.matchIndex[server] >= matchIndex {
				cnt++
			}
		}
		if cnt >= (len(rf.peers) / 2) {
			//fmt.Printf("leader commit, index from %v to %v.\n", rf.commitIndex, matchIndex)
			rf.commitIndex = matchIndex
			rf.applyCond.Signal()
		}
	}

}

// 一轮心跳
func (rf *Raft) leaderHeartBeat(isHeartBeat bool) {
	if !isHeartBeat {
		rf.persist()
	}
	for peer := range rf.peers {
		// 过滤自己
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			// 需要立刻发送心跳内容，以保证当前节点的领导状态
			//fmt.Printf("Leader %v send leader heartBeat.\n", rf.me)
			go rf.replicateOneRound(peer)
		} else {
			// 唤醒replicator，批量发送日志
			//fmt.Printf("Signal replicator %v.\n", peer)
			rf.replicatorCond[peer].Signal()
		}
	}
}

// 实际投票操作
func (rf *Raft) StartElection() {
	request := new(RequestVoteArgs)
	request.Term = rf.currentTerm
	request.CandidateId = rf.me
	request.LastLogTerm = rf.getLastLog().Term
	request.LastLogIndex = rf.getLastLog().Index
	// todo 参数补全
	// 收到的投票数
	grantedVotes := 1
	rf.votedFor = rf.me

	//fmt.Printf("ID: %v start a round of vote, its term is %v.\n", rf.me, rf.currentTerm)
	rf.persist()
	for peer := range rf.peers {
		// 跳过自己
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := new(RequestVoteReply)
			// 利用rpc的返回值，过滤过期请求，在调用send后再统计vote而不是在里面
			//fmt.Printf("ID %v request vote from %v.\n", rf.me, peer)
			if rf.sendRequestVote(peer, request, reply) {
				//fmt.Printf("RequestVote reply!\n")
				//fmt.Printf("voteGranted: %v.\n", reply.VoteGrand)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// double check，避免退回Follower后又变成leader
				if rf.currentTerm == request.Term && rf.state == CANDIDATE {
					//fmt.Printf("server state didnt change. reply.voteGrand: %v.\n", reply.VoteGrand)
					// 统计票数
					if reply.VoteGrand {
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 {
							//fmt.Printf("ID: %v become the new leader. term is %v.\n", rf.me, rf.currentTerm)
							rf.state = LEADER
							// 心跳后立刻开启一轮heartBeat
							//rf.leaderHeartBeat(true)
						}
					} else if reply.Term > rf.currentTerm {
						// 自己过时了，退回follower
						rf.state = FOLLOWER
						rf.votedFor = -1
						rf.currentTerm = reply.Term
						rf.persist()
					}
				}
			}
		}(peer)
	}

	//fmt.Printf("ID: %v receive %v votes.\n", rf.me, grantedVotes)
}

//func (rf *Raft) vote(durationMs int64) {
//	rf.mu.Lock()
//	if rf.state == FOLLOWER {
//		rf.mu.Unlock()
//		return
//	}
//	rf.currentTerm = rf.currentTerm + 1
//	//fmt.Printf("ID: %v term plus one to %v. Because of Start a new vote. Its state is %v. last appendRpc time is %v.\n", rf.me, rf.currentTerm, rf.state, durationMs)
//	rf.state = CANDIDATE
//	rf.voteReceived = 1
//	rf.votedFor = rf.me
//	rf.mu.Unlock()
//
//	for server, _ := range rf.peers {
//		// 过滤自己
//		if server != rf.me {
//			request := new(RequestVoteArgs)
//			reply := new(RequestVoteReply)
//			rf.mu.Lock()
//			request.Term = rf.currentTerm
//			request.CandidateId = rf.me
//			rf.mu.Unlock()
//
//			go rf.sendRequestVote(server, request, reply)
//		}
//	}
//}

// 计时器
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		// 检查收到的term，如果收到的term小于当期的term或者没有收到term，开始一次选举
		// 否则，就继续计时
		// 这里应该是从

		// todo: 这是启动时的暂停时间，还是选举的暂停时间？
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//if rf.state == LEADER {
		//	// LEADER
		//	rf.leaderHeartBeat()
		//	continue
		//}
		//
		//rf.mu.Lock()
		//rf.state = CANDIDATE
		//rf.mu.Unlock()
		//ms := 50 + (rand.Int63() % 300)
		//time.Sleep(time.Duration(ms) * time.Millisecond)
		//
		////fmt.Printf("ID: %v has state %v.\n", rf.me, rf.state)
		//
		//// check if we need a vote
		//if rf.state == LEADER {
		//	// LEADER
		//	rf.leaderHeartBeat()
		//	continue
		//} else if rf.state == FOLLOWER {
		//	continue
		//	// FOLLOWER
		//	//durationMs := int64(time.Since(rf.lastHeartBeat).Milliseconds())
		//	//if durationMs > ((rand.Int63() % 200) + ELECTION_TIMEOUT) {
		//	//	// 已经超时了，需要开始一轮投票
		//	//	//fmt.Printf("ID: %v start a new vote with term %v. since the last heartBeat is %v.\n", rf.me, rf.currentTerm, durationMs)
		//	//	rf.vote()
		//	//}
		//} else {
		//	durationMs := int64(time.Since(rf.lastHeartBeat).Milliseconds())
		//	// CANDIDATE
		//	if durationMs > ((rand.Int63() % 200) + ELECTION_TIMEOUT) {
		//		// 已经超时了，需要开始一轮投票
		//		rf.vote(durationMs)
		//	}
		//}

		select {
		// 选举计时器到期，开始一轮新投票
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.state = CANDIDATE
				rf.currentTerm = rf.currentTerm + 1
				rf.StartElection()
			}
			rf.electionTimer.Reset(RandomElectionTimeout())
			rf.mu.Unlock()
		// 所有server都进行heartBeat倒计时，但只有leader能发送
		case <-rf.heartBeatTimer.C:
			rf.mu.Lock()
			if rf.state == LEADER {
				rf.leaderHeartBeat(true)
			}
			rf.heartBeatTimer.Reset(HeartBeatTimeout())
			rf.mu.Unlock()
		}
	}
}

// 选举时间是随机的
func RandomElectionTimeout() time.Duration {
	ms := 200 + (rand.Int63() % 200)
	return time.Duration(ms) * time.Millisecond
}

// 心跳时间是恒定的
func HeartBeatTimeout() time.Duration {
	return time.Duration(HEARTBEAT_DURATION) * time.Millisecond
}

// 获取最后一个日志
func (rf *Raft) getLastLog() LogEntry {
	return rf.log[len(rf.log)-1]
}

// 获取第一个日志
func (rf *Raft) getFirstLog() LogEntry {
	return rf.log[0]
}

// replicator 协程，用于维护每个节点的复制状态
func (rf *Raft) replicator(peer int) {
	// 在调用Wait()方法之前，必须先调用L.lock()方法来获取互斥锁。
	// 否则，其他goroutine可能会在Wait()方法还没有返回之前，就获取到Cond
	// 条件变量。这将导致Wait()方法无法正确唤醒当前的goroutine。
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()

	for rf.killed() == false {
		// 如果peer节点已经catch，就用信号量暂停
		for !rf.needReplicating(peer) {
			rf.replicatorCond[peer].Wait()
		}
		//fmt.Printf("start replicate %v. mathcIndex: %v, LeaderLastLogIndex: %v.\n", peer, rf.matchIndex[peer], rf.getLastLog().Index)
		rf.replicateOneRound(peer)
	}
}

func (rf *Raft) needReplicating(peer int) bool {
	// 没有修改，只加读锁
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	// 当前节点是leader && peer节点的日志没有catch上leader的日志
	return rf.state == LEADER && rf.matchIndex[peer] < rf.getLastLog().Index
}

// 向applyCh中push提交的日志并保证exactly once
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		// 如果不需要向应用层返回，释放cpu并等待其他协程激活
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		firstIndex, commitIndex, lastApplied := rf.getFirstLog().Index, rf.commitIndex, rf.lastApplied
		if lastApplied < firstIndex {
			rf.lastApplied = firstIndex
			rf.commitIndex = firstIndex
			rf.mu.Unlock()
			continue
		}
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.log[lastApplied+1-firstIndex:commitIndex+1-firstIndex])
		//fmt.Printf("ID: %v apply command. from lastApplied %v to commitIndex %v. firstIndex: %v.\n", rf.me, lastApplied, commitIndex, firstIndex)
		rf.mu.Unlock()
		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
		}
		rf.mu.Lock()
		rf.lastApplied = IntegerMax(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		dead:        0,
		applyCh:     applyCh,
		state:       FOLLOWER,
		currentTerm: 0,
		votedFor:    -1,
		// 日志从下标1开始
		log:            make([]LogEntry, 1),
		nextIndex:      make([]int, len(peers)),
		matchIndex:     make([]int, len(peers)),
		heartBeatTimer: time.NewTimer(HeartBeatTimeout()),
		electionTimer:  time.NewTimer(RandomElectionTimeout()),
		replicatorCond: make([]*sync.Cond, len(peers)),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// 信号量
	rf.applyCond = sync.NewCond(&rf.mu)
	lastLog := rf.getLastLog()
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, lastLog.Index+1
		if i != rf.me {
			rf.replicatorCond[i] = sync.NewCond(&sync.Mutex{})
			// 开启 replicator 协程异步复制日志
			// 分别管理peer的复制状态
			go rf.replicator(i)
		}
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	// start applier goroutine to push committed logs into applyCh exactly once
	// 向applyCh中push提交的日志并保证exactly once
	go rf.applier()

	return rf
}
