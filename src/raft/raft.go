package raft

import (
	"6.5840/labgob"
	"bytes"
	"fmt"
	"sort"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

//节点类型
type Status int

//枚举节点类型
const (
	Follower Status = iota
	Candidate
	Leader
)

const HeartBeatTimeOut = 100 * time.Millisecond


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

type Raft struct {
	mu        		 sync.Mutex          // Lock to protect shared access to this peer's state
	peers     		 []*labrpc.ClientEnd // RPC end points of all peers
	persister 		 *Persister          // Object to hold this peer's persisted state
	me        		 int                 // this peer's index into peers[]
	dead      		 int32               // set by Kill()
	//超时选举
	currentTerm 	 int //最后一次收到的服务的任期
	votedFor 		 int //在当前term下收到选举的candidateId
	voteCount 		 int//当前term收到的投票成功数
	status 			 Status //节点类型
	overtime 		 time.Duration //任期倒计时总长
	timer    		 *time.Ticker //实现倒计时功能
	//日志复制
	logs 			 []LogEntry
	commitIndex 	 int //通过对 matchIndex 排序找中位数的 index ，就是大多数节点都拥有的日志范围，将其设置为 commitIndex 。
	lastApplied 	 int
	nextIndex 		 []int //每个服务器占一个slot，是下一个心跳发送给Follower的index；是Leader对Follower同步的猜测
	matchIndex 		 []int //记录Leader和对应Follower匹配的长度，元素的记录将要复制给机器的日志的索引;实际获知到的同步进度
	applyChan 		 chan ApplyMsg
	//日志快照
	lastIncludeIndex int
	lastIncludeTerm  int
}

type RequestVoteArgs struct {
	//超时选举
	Term 			int //candidate任期
	CandidateId 	int //candidate's index in peers
	//日志复制
	LastLogIndex 	int //最后一个日志的index
	LastLogTerm 	int  //最后一个日志的term
}

type RequestVoteReply struct {
	Term 			int //给candidate发送自己的currentTerm，以便candidate更新自己的currentTerm
	VoteGranted 	bool //是否给candidate投票
}

type AppendEntriesArgs struct{
	//超时选举
	LeaderId 		int
	Term 			int
	//日志复制
	PreLogIndex 	int //前置日志索引，用来确定Leader与Follower从什么index开始日志不同
	PreLogTerm 		int //前置日志任期，用来确定Leader与Follower从什么index开始日志不同
	Entries 		[]LogEntry //Leader从PreLogIndex-1开始的日志
	LeaderCommit 	int //Leader的CommitIndex
}

type AppendEntriesReply struct {
	Success 		bool
	Term 			int
	NextIndex 		int //Leader的nextIndex对应slot的值
}

type LogEntry struct {
	Term 			int
	Command 		interface{}
}

type InstallSnapshotArgs struct{
	Term    		int //Leader's term
	LeaderId 		int
	LastIncludeIndex int
	LastIncludeTerm int
	Data 			[]byte
}

type InstallSnapshotReply struct{
	Term 			int
}

func (args RequestVoteArgs)String()  string{
	return fmt.Sprintf("RequestVoteArgs{Term:%d,CandidateId:%d,LastLogIndex:%d,LastLogTerm:%d}",
		args.Term,args.CandidateId,args.LastLogIndex,args.LastLogTerm)
}
func (args AppendEntriesArgs)String()  string{
	return fmt.Sprintf("AppendEntriesArgs{LeaderId:%d,Term:%d,PreLogIndex:%d,PreLogTerm:%d,Entries:%v,LeaderCommit:%v}",
		args.LeaderId,args.Term,args.PreLogIndex,args.PreLogTerm,args.Entries,args.LeaderCommit)
}
func (logEnrty LogEntry)String()  string{
	return fmt.Sprintf("LogEntry{term:%d,command:%v}",logEnrty.Term,logEnrty.Command)
}
func (applyMsg ApplyMsg)String()  string{
	return fmt.Sprintf("ApplyMsg{CommandValid:%v,CommandIndex:%d,Command:%v}",
		applyMsg.CommandValid,applyMsg.CommandIndex,applyMsg.Command)
}
func (installSnapshotArgs InstallSnapshotArgs)String()  string{
	return fmt.Sprintf("InstallSnapshotArgs{Term:%v,LeaderId:%v,LastIncludeIndex:%v,LastIncludeTerm:%v",
		installSnapshotArgs.Term,installSnapshotArgs.LeaderId,installSnapshotArgs.LastIncludeIndex,installSnapshotArgs.LastIncludeTerm)
}
func (rf *Raft) applyMsgs(msgs []ApplyMsg) {
	for _, msg := range msgs {
		//fmt.Printf("peer[ %d ] 响应消息 %v \n", rf.me, msg)
		rf.applyChan <- msg
	}
}

/*-----------------------------------------------------------ticker------------------------------------------*/
func (rf *Raft) ticker() {
	for rf.killed() == false {
		<-rf.timer.C
		rf.mu.Lock()
		switch rf.status{
		case Follower:
			rf.status=Candidate
			fallthrough
		case Candidate:
			rf.currentTerm++
			rf.overtime=time.Duration(150+rand.Intn(200)) * time.Millisecond
			rf.timer.Reset(rf.overtime)
			rf.votedFor = rf.me
			rf.voteCount=1
			args := RequestVoteArgs{
				Term: rf.currentTerm,
				CandidateId: rf.me,
				LastLogIndex: len(rf.logs) + rf.lastIncludeIndex,
				LastLogTerm: 0,
			}
			//fmt.Printf("%v peer[%d] 在term:%d 一共给 %d 个节点发送选举请求 %v \n",util.PrintTime(),rf.me,rf.currentTerm,len(rf.peers),args)
			for followerId,_ := range rf.peers{
				if followerId == rf.me{continue}
				if len(rf.logs) > 0{
					args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
				}
				go rf.sendRequestVote(followerId, &args,&RequestVoteReply{})
			}
			rf.persist()
			////fmt.Printf("peer[ %d ]发送完选举请求持久化:\n",rf.me)
		case Leader:
			for followerId,_ := range rf.peers{
				if followerId == rf.me{continue}
				appendEntriesArgs := AppendEntriesArgs{
					LeaderId:     rf.me,
					Term:         rf.currentTerm,
					PreLogIndex:  rf.nextIndex[followerId],
					PreLogTerm:   0,
					Entries:      nil,
					LeaderCommit: rf.commitIndex,
				}
				if appendEntriesArgs.PreLogIndex > rf.lastIncludeIndex{
					appendEntriesArgs.PreLogTerm = rf.logs[appendEntriesArgs.PreLogIndex - rf.lastIncludeIndex - 1].Term
					appendEntriesArgs.Entries = rf.logs[appendEntriesArgs.PreLogIndex - rf.lastIncludeIndex:]
					//fmt.Printf("%v peer[%d] 在term:%d 给peer[%d] 发送心跳请求%v \n",util.PrintTime(),rf.me,rf.currentTerm,followerId, appendEntriesArgs)
					go rf.SendAppendEntries(followerId, &appendEntriesArgs,&AppendEntriesReply{})
				}else if appendEntriesArgs.PreLogIndex == rf.lastIncludeIndex{
					appendEntriesArgs.PreLogTerm = rf.lastIncludeTerm
					appendEntriesArgs.Entries = rf.logs[:] //第一次选举成功，next[i]=0,但是接收到命令
					//fmt.Printf("%v peer[%d] 在term:%d 给peer[%d] 发送心跳请求%v \n",util.PrintTime(),rf.me,rf.currentTerm,followerId, appendEntriesArgs)
					go rf.SendAppendEntries(followerId, &appendEntriesArgs,&AppendEntriesReply{})
				}else{
					//TODO 发送Install快照
					installSnapshotArgs := InstallSnapshotArgs{
						Term:             rf.currentTerm,
						LeaderId:         rf.me,
						LastIncludeIndex: rf.lastIncludeIndex,
						LastIncludeTerm:  rf.lastIncludeTerm,
						Data:             rf.persister.ReadSnapshot(),
					}
					//fmt.Printf("%v peer[%d] 在term:%d 给peer[%d] 发送日志快照:%v \n",util.PrintTime(),rf.me,rf.currentTerm,followerId, installSnapshotArgs)
					go rf.sendInstallSnapshot(followerId,&installSnapshotArgs,&InstallSnapshotReply{})
				}
			}
		}
		rf.mu.Unlock()
	}
}
/*----------------------------------------------RPC发送请求---------------------------------------------*/

func (rf *Raft) sendRequestVote(followerId int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[followerId].Call("Raft.RequestVote", args, reply)
	for !ok {ok = rf.peers[followerId].Call("Raft.RequestVote", args, reply)}

	rf.mu.Lock()
	defer rf.mu.Unlock()


	//接收到之前请求的响应
	if rf.currentTerm > args.Term{
		return
	}

	//处理返回结果
	if reply.VoteGranted {
		if rf.voteCount <= len(rf.peers) / 2{
			rf.voteCount++
		}
		if rf.voteCount > len(rf.peers) / 2{
			rf.overtime = HeartBeatTimeOut
			rf.timer.Reset(rf.overtime)
			rf.voteCount = 0 //保证幂等性
			fmt.Printf("peer[ %d ]变为Leader,term为 %d \n",rf.me,rf.currentTerm)
			rf.status = Leader
			//只有选举成功才初始化nextIndex和matchIndex
			for followerId,_ := range rf.peers{
				rf.nextIndex[followerId] = len(rf.logs) + rf.lastIncludeIndex
				rf.matchIndex[followerId] = 0
			}
			rf.matchIndex[rf.me] = len(rf.logs) + rf.lastIncludeIndex
		}
	} else{
			//TODO 应该可以取了
			if rf.currentTerm == reply.Term{//接收端投过票了
				return
			}
			rf.currentTerm = reply.Term
			rf.status = Follower
			rf.voteCount = 0
			rf.votedFor = -1
			rf.persist()
	}
}

func (rf *Raft) SendAppendEntries(followerId int, args *AppendEntriesArgs, reply *AppendEntriesReply) {

	ok := rf.peers[followerId].Call("Raft.AppendEntries", args, reply)
	for !ok {ok = rf.peers[followerId].Call("Raft.AppendEntries", args, reply)}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term || rf.status != Leader || rf.nextIndex[followerId] != args.PreLogIndex{
		return
	}

	if !reply.Success {
		//日志复制失败
		if  rf.currentTerm == reply.Term{
			rf.nextIndex[followerId] = reply.NextIndex
			//fmt.Printf("%v peer[ %d ]的nextIndex[ %d ]更新为 %d \n",util.PrintTime(),rf.me,followerId,reply.NextIndex)
			return
		}
		//出现网络分区
		rf.currentTerm = reply.Term
		rf.status = Follower
		rf.voteCount = 0
		rf.votedFor = -1
		rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
		rf.timer.Reset(rf.overtime)
		rf.persist()
		////fmt.Printf("peer[ %d ]发送心跳失败，持久化\n",rf.me)
	}else{
		rf.nextIndex[followerId] = args.PreLogIndex + len(args.Entries)
		rf.matchIndex[followerId] = args.PreLogIndex + len(args.Entries)
		arr := make([]int,len(rf.matchIndex))
		copy(arr,rf.matchIndex)
		sort.Ints(arr)
		//fmt.Printf("%v peer[ %d ] 给peer[ %d ]发送完消息后的matchIndex为 %v ,排序后为 %v \n",util.PrintTime(),rf.me,followerId,rf.matchIndex,arr)
		newCommitIndex := arr[len(arr)/2]
		msgs := make([]ApplyMsg,0)
		if newCommitIndex > rf.commitIndex{
			for rf.commitIndex < newCommitIndex{
				if rf.commitIndex >= rf.lastIncludeIndex{
					msgs = append(msgs,ApplyMsg{
						CommandValid: true,
						CommandIndex: rf.commitIndex + 1,
						Command: rf.logs[rf.commitIndex - rf.lastIncludeIndex].Command,
					})
				}
				rf.commitIndex++
				rf.lastApplied = rf.commitIndex
			}
		}
		go rf.applyMsgs(msgs)
	}
}

func (rf *Raft) sendInstallSnapshot(followerId int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ok := rf.peers[followerId].Call("Raft.InstallSnapshot", args, reply)
	for !ok {ok = rf.peers[followerId].Call("Raft.InstallSnapshot", args, reply)}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term{
		return
	}
	if rf.currentTerm >= reply.Term{
		rf.nextIndex[followerId] = rf.lastIncludeIndex
		return
	}
	rf.currentTerm = reply.Term
	rf.status = Follower
	rf.voteCount = 0
	rf.votedFor = -1
	rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
	rf.timer.Reset(rf.overtime)
	rf.persist()
}




/*-----------------------------------------------RPC处理请求---------------------------------------*/

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//Candidate的term比自己小，或者自己已经投过票
	if args.Term <= rf.currentTerm {
		//fmt.Printf("%v peer[ %d ] 在term:%d 时因为不满足任期要求拒绝peer[ %d ]发送的选举请求RPC %v \n",util.PrintTime(),rf.me,rf.currentTerm,args.CandidateId,args)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	rf.currentTerm = args.Term
	rf.status = Follower
	rf.persist()

	//1.如果Candidate的日志最后的term比自己的小
	//2.最后日志的term相等，但是Candidate最后日志的index比自己的小
	//拒绝投票
	if len(rf.logs) > 0 &&
		(args.LastLogTerm < rf.logs[len(rf.logs)-1].Term || (args.LastLogTerm ==  rf.logs[len(rf.logs)-1].Term && args.LastLogIndex < len(rf.logs) + rf.lastIncludeIndex)){
		//fmt.Printf("%v peer[ %d ] 在term:%d 时因为不满足日志要求拒绝peer[ %d ]发送的选举请求RPC%v ,而其log长度为 %v,lastIncluedIndex为%d \n",util.PrintTime(),rf.me,rf.currentTerm,args.CandidateId,args,len(rf.logs),rf.lastIncludeIndex)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	//fmt.Printf("%v peer[ %d ] 在term:%d 时成功处理了peer[ %d ]发送的选举请求RPC %v \n",util.PrintTime(),rf.me,rf.currentTerm,args.CandidateId,args)
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
	rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
	rf.timer.Reset(rf.overtime)
	rf.votedFor = args.CandidateId
	rf.persist()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//Leader的term比自己的还小
	if rf.currentTerm > args.Term{
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.NextIndex = -1
		return
	}

	rf.currentTerm = args.Term
	rf.status = Follower
	rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
	rf.timer.Reset(rf.overtime)
	rf.persist()

	if args.PreLogIndex < rf.lastIncludeIndex{
		//fmt.Printf("%v peer[ %d ] 的PreLogIndex %d 小于peer[ %d ]的lastIncludeIndex %d \n",util.PrintTime(),args.LeaderId,args.PreLogIndex,rf.me,rf.lastIncludeIndex)
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.lastIncludeIndex
		return
	}

	//日志冲突
	//1. 自己logs在preLogIndex处没有日志
	if args.PreLogIndex > rf.lastIncludeIndex && args.PreLogIndex > rf.lastIncludeIndex + len(rf.logs){
		//fmt.Printf("%v peer[ %d ] 在term:%d 时在 %d 处没有日志 \n",util.PrintTime(),rf.me,rf.currentTerm,args.PreLogIndex - 1)
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.NextIndex = len(rf.logs) + rf.lastIncludeIndex
		return
	}

	//2.自己与Leader在preLogIndex处的日志不相同
	if args.PreLogIndex > rf.lastIncludeIndex && rf.logs[args.PreLogIndex - rf.lastIncludeIndex - 1].Term != args.PreLogTerm{
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.NextIndex = (args.PreLogIndex + rf.lastIncludeIndex) / 2
		//fmt.Printf("%v peer[ %d ] 在term:%d 时日志在 %d 处与Leader的日志不同,PreLogIndex更新为%d \n",util.PrintTime(),rf.me,rf.currentTerm,args.PreLogIndex - 1,reply.NextIndex)
		return
	}

	if args.Entries != nil && len(args.Entries) > 0{
		logs := rf.logs[:args.PreLogIndex - rf.lastIncludeIndex]
		rf.logs = append(logs,args.Entries...)
	}

	rf.persist()
	//fmt.Printf("%v peer[ %d ] 接收到心跳，其日志长度变为 %d ,并且lastIncluedIndex为 %d\n",util.PrintTime(),rf.me,len(rf.logs),rf.lastIncludeIndex)
	msgs := make([]ApplyMsg,0)
	for rf.commitIndex < args.LeaderCommit{
		if rf.commitIndex >= rf.lastIncludeIndex{
			msgs = append(msgs,ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.commitIndex - rf.lastIncludeIndex].Command,
				CommandIndex: rf.commitIndex + 1,
			})
		}
		rf.commitIndex++
		rf.lastApplied = rf.commitIndex
	}
	go rf.applyMsgs(msgs)
	reply.Success = true
	reply.Term = rf.currentTerm
	reply.NextIndex = len(rf.logs) + rf.lastIncludeIndex
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply){
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm{
		reply.Term = rf.currentTerm
		return
	}
	rf.currentTerm = args.Term
	rf.status = Follower
	rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
	rf.timer.Reset(rf.overtime)
	rf.persist()
	if args.LastIncludeIndex <= rf.lastIncludeIndex{
		reply.Term = rf.currentTerm
		return
	}
	//fmt.Printf("%v peer[%d] 处理peer[%d] 发送日志快照:%v \n",util.PrintTime(),rf.me,args.LeaderId,args)
	if args.LastIncludeIndex >= rf.lastIncludeIndex + len(rf.logs){
		rf.logs = make([] LogEntry,0)
	}else{
		rf.logs = rf.logs[args.LastIncludeIndex - rf.lastIncludeIndex:len(rf.logs)]
	}
	if args.LastIncludeIndex > rf.commitIndex{
		rf.commitIndex = args.LastIncludeIndex
	}
	if args.LastIncludeIndex > rf.lastApplied{
		rf.lastApplied = args.LastIncludeIndex
	}
	rf.lastIncludeIndex = args.LastIncludeIndex
	rf.lastIncludeTerm = args.LastIncludeTerm
	rf.persister.Save(rf.persistData(), args.Data)
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludeIndex,
		SnapshotTerm:  args.LastIncludeTerm,
	}
	go func() {rf.applyChan <- msg}()
}

/*---------------------------------------外部调用检查Raft状态--------------------------------------*/


func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.status == Leader
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return -1,-1,false
	}
	if rf.status!=Leader{
		return -1,-1,false
	}
	rf.logs = append(rf.logs,LogEntry{rf.currentTerm,command})
	//fmt.Printf("peer[ %d ]添加日志 %v \n",rf.me,command)
	rf.persist()
	////fmt.Printf("peer[ %d ]添加日志后持久化\n",rf.me)
	rf.matchIndex[rf.me] = len(rf.logs) + rf.lastIncludeIndex
	index = len(rf.logs) + rf.lastIncludeIndex
	term = rf.currentTerm
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	//fmt.Println("peer[",rf.me,"]kill")
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) persistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	data := w.Bytes()
	return data
}

func (rf *Raft) persist() {
	rf.persister.Save(rf.persistData(), rf.persister.ReadSnapshot())
}

func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []LogEntry
	var lastIncludeIndex int
	var lastIncludeTerm int
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logs) != nil ||
		d.Decode(&lastIncludeIndex) != nil || d.Decode(&lastIncludeTerm) != nil{
		////fmt.Printf("持久化解码错误\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.logs = logs
		rf.lastIncludeIndex = lastIncludeIndex
		rf.lastIncludeTerm = lastIncludeTerm
		//fmt.Printf("%v Decode peer[ %d ]:rf.currentTerm: %d, rf.votedFor: %d, rf.logs: %v ,rf.lastIncludeIndex: %d\n",
			//util.PrintTime(),rf.me,rf.currentTerm,rf.votedFor,rf.logs,rf.lastIncludeIndex)
	}
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	if rf.killed(){
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludeIndex || index > rf.commitIndex{
		return
	}
	//fmt.Printf("%v peer[%d]在index：%d处生成快照 %v\n",util.PrintTime(),rf.me,index,snapshot)
	rf.lastIncludeTerm = rf.logs[index - rf.lastIncludeIndex - 1].Term
	rf.logs = rf.logs[index - rf.lastIncludeIndex:len(rf.logs)]
	rf.lastIncludeIndex = index
	//fmt.Printf("%v 生成快照后peer[%d]status为%v，logs变为%v\n",util.PrintTime(),rf.me,rf.status,rf.logs)
	rf.persister.Save(rf.persistData(),snapshot)
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyChan = applyCh
	//超时选举
	rf.votedFor = -1
	rf.voteCount = 0
	rf.currentTerm = 0
	//日志复制
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int,len(rf.peers))
	rf.matchIndex = make([]int, len(peers))
	rf.logs = make([]LogEntry,0)
	//日志快照
	rf.lastIncludeIndex = 0
	rf.lastIncludeTerm = 0
	//日志持久化
	rf.readPersist(persister.ReadRaftState())
	rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond
	rf.timer = time.NewTicker(rf.overtime)
	go rf.ticker()
	return rf
}
