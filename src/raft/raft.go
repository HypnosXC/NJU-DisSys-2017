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
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
	//"bytes"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

const (
	Follower  int = 1
	Candidate     = 2
	Leader        = 3
)

//
// A Go object implementing a single Raft peer.

type LogEntry struct {
	Command interface{}
	Term    int
}
type AppendEntryArgs struct {
	LeaderTerm   int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entry        []LogEntry
	LeaderCommit int
}
type AppendEntryReply struct {
	Term    int
	Success bool
}
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd //RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisited statee
	me        int                 // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state int
	// election used
	voteInd     int
	voteCount   int
	currentTerm int

	//Log entry
	logs []LogEntry

	//State info
	CommitIndex int
	LastApplied int

	//leader statement
	NextIndex  []int
	MatchIndex []int
	//channels
	chanApply     chan ApplyMsg
	chanHeartbeat chan bool
	chanWinElect  chan bool
	chanGrantVote chan bool
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	isleader := false
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	}
	return term, isleader
}
func (rf *Raft) TurnState(state int, term int) {
	rf.state = state
	rf.voteCount = 0
	rf.voteInd = -1
	if state == Follower { // Turn state into Follower
		rf.currentTerm = term
	}
	if state == Leader {
		rf.NextIndex = make([]int, len(rf.peers))
		rf.MatchIndex = make([]int, len(rf.peers))
		for index := range rf.NextIndex {
			rf.NextIndex[index] = len(rf.logs)
		}
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) LogToUpdate(args RequestVoteArgs) bool {
	Isupdate := false
	tail := len(rf.logs) - 1
	logterm := rf.logs[tail].Term
	if args.LastLogTerm >= logterm {
		if args.LastLogIndex >= tail {
			Isupdate = true
		}
	}
	return Isupdate
}
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term { // bigger term ,candidate turn to followers
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	DPrintf("Voting : %d vote to candidate %d", rf.me, args.CandidateId)
	if rf.currentTerm < args.Term { // less term, rf become follower of such candidate
		rf.TurnState(Follower, args.Term)
	}
	reply.Term = args.Term // equal term
	if rf.voteInd == -1 && rf.LogToUpdate(args) {
		rf.voteInd = args.CandidateId
		reply.VoteGranted = true
		rf.chanGrantVote <- true
	} else {
		reply.VoteGranted = false
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != Candidate {
			//invaild request
			return ok
		}
		if reply.Term > rf.currentTerm { // bigger term, converse it to follower
			rf.TurnState(Follower, reply.Term)
			return ok
		}
		if reply.VoteGranted == true { // Granted
			rf.voteCount++
			DPrintf("Voting : Candidate %d get votes at %d", rf.me, rf.voteCount)
			if rf.voteCount > len(rf.peers)/2 { //become leader
				rf.TurnState(Leader, rf.currentTerm)
				rf.chanWinElect <- true
			}
		}
	}
	return ok
}

func (rf *Raft) BroadcastRequestVote() {
	rf.mu.Lock()
	args := &RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = len(rf.logs) - 1
	args.LastLogTerm = rf.logs[args.LastLogIndex].Term
	rf.mu.Unlock()
	for server := range rf.peers {
		if server != rf.me && rf.state == Candidate {
			go rf.sendRequestVote(server, *args, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) RequestAppend(args AppendEntryArgs, reply *AppendEntryReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("rf %d (term %d) get append from leader %d (term %d)", rf.me, rf.currentTerm, args.LeaderId, args.LeaderTerm)
	if args.LeaderTerm < rf.currentTerm { //1
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if rf.currentTerm < args.LeaderTerm { // less term, rf become follower of leader
		rf.TurnState(Follower, args.LeaderTerm)
	}
	reply.Term = args.LeaderTerm
	reply.Success = false
	rf.chanHeartbeat <- true
	tail := len(rf.logs) - 1
	if tail < args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm { //2
		DPrintf("False append: rf:%d(term %d) from %d,logs %v", rf.me, rf.currentTerm, args.LeaderId, rf.logs)
		return
	}
	if len(args.Entry) > 0 { // success copy
		DPrintf("success append: rf:%d(term %d),leader %d(term %d)", rf.me, rf.currentTerm, args.LeaderId, args.LeaderTerm)
		rf.logs = rf.logs[:args.PrevLogIndex+1]
		rf.logs = append(rf.logs, args.Entry...)
	}
	if args.LeaderCommit > rf.CommitIndex {
		rf.CommitIndex = min(args.LeaderCommit, len(rf.logs)-1)
		go rf.applyLog()
	}
	reply.Success = true
}
func (rf *Raft) applyLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.LastApplied + 1; i <= rf.CommitIndex; i++ {
		msg := ApplyMsg{}
		msg.Index = i
		msg.Command = rf.logs[i].Command
		rf.chanApply <- msg
	}
	//fmt.Println(rf.me, rf.logs, rf.LastApplied, rf.CommitIndex)
	rf.LastApplied = rf.CommitIndex
}
func (rf *Raft) sendAppendEntry(server int, args AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppend", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if rf.state != Leader {
			//invaild append
			return ok
		}
		if reply.Term > rf.currentTerm { // bigger term, converse it to follower
			rf.TurnState(Follower, reply.Term)
			return ok
		}
		if reply.Success { // Success commit
			if len(args.Entry) > 0 { // no heartbeat
				rf.NextIndex[server] = len(rf.logs)
				rf.MatchIndex[server] = len(rf.logs) - 1
			}
		} else { // adjust nextindex
			prevind := args.PrevLogIndex
			prevterm := args.PrevLogTerm
			for prevind >= 0 {
				if rf.logs[prevind].Term == prevterm {
					prevind--
				} else {
					break
				}
			}
			fmt.Println("Adjust to", prevind)
			rf.NextIndex[server] = prevind + 1
		}
		tail := len(rf.logs) - 1
		for i := tail; i > rf.CommitIndex; i-- {
			cnt := 1
			for j := range rf.peers {
				if j != rf.me && rf.MatchIndex[j] >= i {
					cnt++
				}
			}
			if cnt > len(rf.peers)/2 { // successfully commit
				rf.CommitIndex = i
				go rf.applyLog()
				break
			}
		}
	}
	return ok
}

func (rf *Raft) BroadcastAppendEntry() {
	for server := range rf.peers {
		rf.mu.Lock()
		args := &AppendEntryArgs{}
		args.LeaderTerm = rf.currentTerm
		args.LeaderCommit = rf.CommitIndex
		args.LeaderId = rf.me
		args.Entry = rf.logs[rf.NextIndex[server]:]
		args.PrevLogIndex = rf.NextIndex[server] - 1
		args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		fmt.Println("Leader", rf.me, "with ", rf.logs, "broadcast to", server)
		rf.mu.Unlock()
		if server != rf.me && rf.state == Leader {
			go rf.sendAppendEntry(server, *args, &AppendEntryReply{})
		}
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := -1
	term := -1
	isLeader := false
	isLeader = (rf.state == Leader)
	if isLeader { // leader send interface
		index = len(rf.logs)
		rf.logs = append(rf.logs, LogEntry{Command: command, Term: rf.currentTerm})
		term = rf.currentTerm
		fmt.Println("leader is", rf.me, "log now is", command, rf.NextIndex, rf.MatchIndex)
	}
	return index, term, isLeader
}
func (rf *Raft) run() {
	for {
		switch rf.state {
		case Follower:
			select {
			case <-rf.chanGrantVote:
			case <-rf.chanHeartbeat:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(300)+200)):
				rf.TurnState(Candidate, rf.currentTerm)
			}
		case Leader:
			go rf.BroadcastAppendEntry()
			time.Sleep(time.Millisecond * 60)
		case Candidate:
			rf.mu.Lock()
			rf.currentTerm++
			rf.voteCount = 1
			rf.voteInd = rf.me
			rf.mu.Unlock()
			fmt.Println("rf", rf.me, "can", rf.currentTerm)
			go rf.BroadcastRequestVote()

			select {
			case <-rf.chanHeartbeat:
				rf.state = Follower
			case <-rf.chanWinElect:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(300)+200)):
			}

		}
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.TurnState(Follower, 0)

	rf.logs = append(rf.logs, LogEntry{Term: 0})

	rf.LastApplied = 0
	rf.CommitIndex = 0

	rf.chanApply = applyCh
	rf.chanGrantVote = make(chan bool, 100)
	rf.chanHeartbeat = make(chan bool, 100)
	rf.chanWinElect = make(chan bool, 100)
	// Your initialization code here.

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.run()
	return rf
}
