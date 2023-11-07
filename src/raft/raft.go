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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 2A
	currentTerm   int       // 当前的任期 需要持久化
	votedFor      int       //
	state         int       // 应该保存state，至少知道自己的状态吧？
	voteReceived  int       // 当前term中，当前节点收到的选票数目
	lastHeartBeat time.Time // 最后一次心跳的时间
	lastTicker    int       //
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
	Term      int // currentTerm, for candidate to update itself
	VoteGrand int // true means candidate received vote
}

// example RequestVote RPC handler.
// 收到RequestVote后的处理方法
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 1. 检查Term是否大于自己，如果大于，放弃选举，给候选人投票，更新term

	if args.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		//fmt.Printf("ID: %v term changed to %v. Because of Receive RequestVote RPC.\n", rf.me, rf.currentTerm)
		rf.state = FOLLOWER
		rf.voteReceived = 0
		rf.votedFor = args.CandidateId
		rf.mu.Unlock()
		reply.Term = rf.currentTerm
		reply.VoteGrand = 1
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGrand = 0
	}

	// 如果不大于，
	// 2. 检查日志是否比自己新
	// 比自己新，

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
	// 如果发别其他节点的term更高，自己降为follower，更新term
	if rf.currentTerm < reply.Term {
		rf.mu.Lock()
		rf.state = FOLLOWER
		rf.currentTerm = reply.Term
		//fmt.Printf("ID: %v term changed to %v. Because of sendRequestVote RPC.\n", rf.me, rf.currentTerm)
		rf.mu.Unlock()
	} else {
		// 增加当前的选票
		rf.mu.Lock()
		if reply.VoteGrand == 1 && rf.state != LEADER {
			rf.voteReceived++
			n := len(rf.peers)
			if rf.voteReceived > n/2 {
				rf.state = LEADER
				//fmt.Printf("ID: %v is new Leader. Its term is %v.\n", rf.me, rf.currentTerm)
			}

		}
		rf.mu.Unlock()
	}

	return ok
}

type AppendEntriesRequst struct {
	Term         int // leader's term
	LeaderId     int // index of leader
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []int
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int  // terms of other servers
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesRequst, reply *AppendEntriesReply) {
	//fmt.Printf("ID: %v receive new Leader HeartBeat. leaderID: %v, leaderTerm: %v.\n", rf.me, args.LeaderId, args.Term)
	if args.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.currentTerm = args.Term
		//fmt.Printf("ID: %v term changed to %v. Because of Receive AppendLog RPC.\n", rf.me, rf.currentTerm)
		rf.state = FOLLOWER
		rf.mu.Unlock()
	}

	if rf.state == FOLLOWER {
		rf.mu.Lock()
		rf.lastHeartBeat = time.Now()
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
	} else if rf.state == CANDIDATE {
		// leader 的 term大于等于自己，竞选失败
		rf.mu.Lock()
		rf.state = FOLLOWER
		rf.lastHeartBeat = time.Now()
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
	} else {
		// todo: 考虑 log
		// 相等term统一处理成退回
		if rf.currentTerm == args.Term {
			//rf.mu.Lock()
			//rf.state = FOLLOWER
			//rf.currentTerm = args.Term
			//rf.mu.Unlock()
		}
		reply.Term = rf.currentTerm
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequst, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// 如果发别其他节点的term更高，自己降为follower，更新term
	if rf.currentTerm < reply.Term {
		rf.mu.Lock()
		rf.state = FOLLOWER
		rf.currentTerm = reply.Term
		//fmt.Printf("ID: %v term changed to %v. Because of Send AppendLog RPC.\n", rf.me, rf.currentTerm)
		rf.mu.Unlock()
		//fmt.Printf("ID: %v, term: %v was leader but found %v term higher.\n", rf.me, rf.currentTerm, reply.Term)
	}
	return ok
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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

func (rf *Raft) leaderHeartBeat() {
	for rf.killed() == false && rf.state == LEADER {
		//fmt.Printf("Leader %v start a round of heartBeat.\n", rf.me)

		for server, _ := range rf.peers {
			request := new(AppendEntriesRequst)
			reply := new(AppendEntriesReply)
			rf.mu.Lock()
			request.LeaderId = rf.me
			request.Term = rf.currentTerm
			rf.mu.Unlock()
			// 过滤自己
			if server != rf.me {
				go rf.sendAppendEntries(server, request, reply)
			}
		}

		// 每隔100ms发送一次心跳
		time.Sleep(time.Duration(HEARTBEAT_DURATION) * time.Millisecond)
	}
}

// 实际投票操作
func (rf *Raft) vote(durationMs int64) {
	rf.mu.Lock()
	if rf.state == FOLLOWER {
		rf.mu.Unlock()
		return
	}
	rf.currentTerm = rf.currentTerm + 1
	//fmt.Printf("ID: %v term plus one to %v. Because of Start a new vote. Its state is %v. last appendRpc time is %v.\n", rf.me, rf.currentTerm, rf.state, durationMs)
	rf.state = CANDIDATE
	rf.voteReceived = 1
	rf.votedFor = rf.me
	rf.mu.Unlock()

	for server, _ := range rf.peers {
		// 过滤自己
		if server != rf.me {
			request := new(RequestVoteArgs)
			reply := new(RequestVoteReply)
			rf.mu.Lock()
			request.Term = rf.currentTerm
			request.CandidateId = rf.me
			rf.mu.Unlock()

			go rf.sendRequestVote(server, request, reply)
		}
	}
}

// 计时器 && 投票
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
		if rf.state == LEADER {
			// LEADER
			rf.leaderHeartBeat()
			continue
		}

		rf.mu.Lock()
		rf.state = CANDIDATE
		rf.mu.Unlock()
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		//fmt.Printf("ID: %v has state %v.\n", rf.me, rf.state)

		// check if we need a vote
		if rf.state == LEADER {
			// LEADER
			rf.leaderHeartBeat()
			continue
		} else if rf.state == FOLLOWER {
			continue
			// FOLLOWER
			//durationMs := int64(time.Since(rf.lastHeartBeat).Milliseconds())
			//if durationMs > ((rand.Int63() % 200) + ELECTION_TIMEOUT) {
			//	// 已经超时了，需要开始一轮投票
			//	//fmt.Printf("ID: %v start a new vote with term %v. since the last heartBeat is %v.\n", rf.me, rf.currentTerm, durationMs)
			//	rf.vote()
			//}
		} else {
			durationMs := int64(time.Since(rf.lastHeartBeat).Milliseconds())
			// CANDIDATE
			if durationMs > ((rand.Int63() % 200) + ELECTION_TIMEOUT) {
				// 已经超时了，需要开始一轮投票
				rf.vote(durationMs)
			}
		}

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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	/* 2A
	刚开始都是candidate，并发起一次选举
	*/
	rf.state = FOLLOWER
	//fmt.Println("server start!")

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}