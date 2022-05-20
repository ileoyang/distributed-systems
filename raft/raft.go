package raft

import (
	"distributed-systems/rpcutil"
	"math/rand"
	"sync"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type Raft struct {
	mu          sync.Mutex
	peers       []*rpcutil.ClientEnd
	me          int
	currentTerm int
	votedFor    int
	state       State
	voteCount   int
	stepDownCh  chan bool
	grantVoteCh chan bool
	winElectCh  chan bool
	heartbeatCh chan bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = args.Term == rf.currentTerm && (rf.votedFor < 0 || rf.votedFor == args.CandidateId)
	if reply.VoteGranted {
		rf.votedFor = args.CandidateId
		rf.grantVoteCh <- true
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) {
	reply := &RequestVoteReply{}
	if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
	}
	if rf.state == Candidate && args.Term == rf.currentTerm && reply.Term == rf.currentTerm && reply.VoteGranted {
		rf.voteCount++
		if rf.voteCount == len(rf.peers)/2+1 {
			rf.winElectCh <- true
		}
	}
}

func (rf *Raft) broadcastRequestVote() {
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	for server := range rf.peers {
		if server != rf.me {
			go rf.sendRequestVote(server, &args)
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if reply.Success = args.Term == rf.currentTerm; reply.Success {
		rf.heartbeatCh <- true
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader && args.Term == rf.currentTerm && reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
	}
}

func (rf *Raft) broadcastAppendEntries() {
	for server := range rf.peers {
		if server != rf.me {
			args := AppendEntriesArgs{
				Term: rf.currentTerm,
			}
			go rf.sendAppendEntries(server, &args)
		}
	}
}

func (rf *Raft) convertToFollower(term int) {
	state := rf.state
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	if state != Follower {
		rf.stepDownCh <- true
	}
}

func (rf *Raft) convertToLeader() {
	rf.state = Leader
	rf.broadcastAppendEntries()
}

func (rf *Raft) convertToCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.broadcastRequestVote()
}

func getElectionTimeout() time.Duration {
	return time.Duration(360+rand.Intn(240)) * time.Millisecond
}

func (rf *Raft) runServer() {
	for {
		switch rf.state {
		case Follower:
			select {
			case <-rf.grantVoteCh:
			case <-rf.heartbeatCh:
			case <-time.After(getElectionTimeout()):
				rf.convertToCandidate()
			}
		case Candidate:
			select {
			case <-rf.stepDownCh:
			case <-rf.winElectCh:
				rf.convertToLeader()
			case <-time.After(getElectionTimeout()):
				rf.convertToCandidate()
			}
		case Leader:
			select {
			case <-rf.stepDownCh:
			case <-time.After(100 * time.Millisecond):
				rf.broadcastAppendEntries()
			}
		}
	}
}

func Make(peers []*rpcutil.ClientEnd, me int) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.voteCount = 0
	rf.stepDownCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.winElectCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
	go rf.runServer()
	return rf
}
