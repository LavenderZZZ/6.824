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

const HeartbeatInterval = 10 * time.Millisecond

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

	applyChan chan ApplyMsg // 用于提交日志的通道
	state     State         // 节点状态

	currentTerm int        // 节点当前任期
	votedFor    int        // 当前任期内收到选票的候选人id
	logs        []LogEntry // 日志条目
	commitIndex int        // 已提交的日志条目索引
	lastApplied int        // 已应用到状态机的日志条目索引
	nextIndex   []int      // 对于每个服务器，需要发送给他的下一个日志条目的索引
	matchIndex  []int      // 对于每个服务器，已经复制到该服务器的日志条目的最高索引

	electionTimer *time.Timer // 选举超时定时器
	heartbeat     *time.Timer // 心跳定时器
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type LogEntry struct {
	Command interface{} //日志的具体命令
	Term    int         //日志任期
}

// 追加条目 RPC 参数 , 由领导人调用，用于日志条目的复制，同时也被当做心跳使用
type AppendEntriesArgs struct {
	Term         int        // 领导人的任期号
	LeaderId     int        // 领导人的 Id，以便于跟随者重定向请求
	PrevLogIndex int        // 新的日志条目紧随之前的索引值
	PrevLogTerm  int        // PrevLogIndex 条目的任期号
	Entries      []LogEntry // 准备存储的日志条目（表示心跳时为空；一次性发送多个是为了提高效率）
	LeaderCommit int        // 领导人已经提交的日志的索引值
}

type AppendEntriesReply struct {
	Term    int  // 当前任期，用于领导人更新自己
	Success bool // 跟随者包含了匹配上 PrevLogIndex 和 PrevLogTerm 的日志时为真
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
}

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
	Term         int // 候选人的任期号
	CandidateId  int // 请求选票的候选人的 Id
	LastLogIndex int // 候选人的最后日志条目的索引值
	LastLogTerm  int // 候选人最后日志条目的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool // 候选人赢得了此张选票时为真
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//如果args的term小于当前节点任期或者两者任期相等但是已经投过票且投票节点不是发起者{
	//	返回结果，reply.voteGranted = false;
	//}
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	//如果args.term 大于当前节点任期，说明当前节点已经落后 {
	//	改变自身状态为Follower
	//	重置节点自身的term和投票结果votedFor
	//}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	//如果当前节点没有投过票或者已经投过票且投票节点是发起者且发起者的日志比当前节点新{
	//	更新当前节点的任期
	//	更新当前节点的投票结果
	//	返回结果，reply.voteGranted = true;
	//}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId && args.LastLogIndex >= rf.commitIndex {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
	}

	//如果当前节点的任期大于发起者的任期{
	//	返回结果，reply.voteGranted = false;
	//}
	if (args.LastLogTerm > rf.logs[len(rf.logs)-1].Term) || (args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex >= len(rf.logs)-1) {
		rf.commitIndex = args.LastLogIndex
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		return
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
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

// server is the index of the target server in rf.peers[].
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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

func (rf *Raft) resetElectionTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 生成150到300毫秒之间的随机超时时间
	timeoutDuration := time.Millisecond * time.Duration(150+rand.Intn(150))
	// 重置选举定时器
	if rf.electionTimer == nil {
		rf.electionTimer = time.NewTimer(timeoutDuration)
	} else {
		rf.electionTimer.Reset(timeoutDuration)
	}
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 生成150到300毫秒之间的随机超时时间
	timeoutDuration := time.Millisecond * HeartbeatInterval
	// 重置选举定时器
	if rf.electionTimer == nil {
		rf.electionTimer = time.NewTimer(timeoutDuration)
	} else {
		rf.electionTimer.Reset(timeoutDuration)
	}
}

func (rf *Raft) HasFired(TimerType int) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if TimerType == 0 {
		select {
		case <-rf.electionTimer.C:
			return true
		default:
			return false
		}
	} else {
		select {
		case <-rf.heartbeat.C:
			return true
		default:
			return false
		}
	}
}

const electionType = 0
const heartbeatType = 1

// Your code here (2A)
// Check if a leader election should be started.
// 1.当选举时间超时，触发选举机制：
//
//	节点改变状态为Candidate
//	节点当前任期+1
//	进行选举StartElection()
//	重置选举时间
//
// 2.当心跳时间超时，触发心跳机制：
//
//	发送心跳BroadcastHeartBeat()
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		switch rf.state {
		case Follower:
			if rf.HasFired(electionType) {
				rf.state = Candidate
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.StartElection()
				rf.resetElectionTimer()
			}
		case Leader:
			if rf.HasFired(heartbeatType) {
				rf.BroadcastHeartBeat()
				rf.resetHeartbeatTimer()
			}
		}
	}
	rf.mu.Unlock()
	// pause for a random amount of time between 50 and 350
	// milliseconds.
	ms := 50 + (rand.Int63() % 300)
	time.Sleep(time.Duration(ms) * time.Millisecond)
}

func (rf *Raft) StartElection() {

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.commitIndex,
		LastLogTerm:  rf.logs[rf.commitIndex].Term,
	}
	for i := 0; i < len(rf.peers); i++ {
		//发起一个协程向各个节点通过RPC发起投票请求
		//发起一个协程向各个节点通过RPC发起投票请求{
		//	如果当前节点任期和args的任期参数相同且状态为Candidate则进行后续判断{
		//		如果返回结果正确{
		//			则统计票数
		//			当票数大于一般时转换为Leader节点，发起心跳。
		//		}
		//		如果返回结果任期大于当前节点任期则转换为Follower节点
		//	}
		//}
		if i != rf.me {
			if rf.state == Candidate && rf.currentTerm == args.Term {
				go func(i int) {
					reply := RequestVoteReply{}
					ok := rf.sendRequestVote(i, &args, &reply)
					if ok {
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.state = Follower
							rf.votedFor = -1
						} else if reply.VoteGranted {
							rf.votedFor++
							if rf.votedFor > len(rf.peers)/2 {
								rf.state = Leader
								rf.BroadcastHeartBeat()
							}
						}
					}
				}(i)
			}

		}

	}

}

func (rf *Raft) BroadcastHeartBeat() {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.lastApplied,
		PrevLogTerm:  rf.logs[rf.commitIndex].Term,
		Entries:      rf.logs[rf.commitIndex:],
		LeaderCommit: rf.commitIndex,
	}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(i int) {
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, &args, &reply)
				if ok {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.votedFor = -1
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}
	reply.Term = rf.currentTerm
	reply.Success = true
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
// me argument is the index of this peer in the peers array.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyChan = applyCh
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
