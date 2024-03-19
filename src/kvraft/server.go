package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false
const OutTime = 100 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	//这两个用来比较kv.maxSeq,来去重
	SeqId int64
	ClientId int64
	//执行的操作
	Key string
	Value string
	OpType string
}

func (op Op)String()  string{
	return fmt.Sprintf("Op{SeqId:%v,Key:%v,Value:%v,OpType:%v}",
		op.SeqId,op.Key,op.Value,op.OpType)

}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvPersist map[string] string //持久化key/value对
	waitChMap map[int] chan Op //当多个client并发RPC调用server方法时，可能与raft放入channel的返回值不一致,该属性是建立command与一个新建的channel映射
	maxSeq map[int64] int64 //每个clientId
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{SeqId: args.SeqId, Key: args.Key, OpType: "get",ClientId:args.ClientId}

	commandIndex, _, isLeader:= kv.rf.Start(op)
	if !isLeader{
		reply.Err = ErrWrongLeader
		return
	}
	waitCh := kv.waitChMap[commandIndex]
	fmt.Println(kv.me,"is Leader")
	select {
	case op := <-waitCh:
		reply.Value,reply.Err = kv.getValue(op.Key),OK
	case <-time.After(OutTime):
		reply.Value,reply.Err = "",ErrOutTime
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{SeqId: args.SeqId, Key: args.Key,Value:args.Value, OpType: args.OpType,ClientId:args.ClientId}
	commandIndex, _, isLeader:= kv.rf.Start(op)
	if !isLeader{
		reply.Err = ErrWrongLeader
		return
	}
	waitCh := kv.getWaitCh(commandIndex)
	select {
	case <-waitCh:
		fmt.Println("success")
		reply.Err = OK
	case <-time.After(OutTime):
		reply.Err =ErrOutTime
	}
}

func (kv *KVServer) ApplyMsgHandlerLoop(){
	for !kv.killed(){
		msg := <- kv.applyCh
		fmt.Printf("server[%d]获取到msg:%v \n",kv.me,msg)
		op := msg.Command.(Op)
		if kv.ifDuplicate(op.ClientId,op.SeqId){
			continue
		}
		if op.OpType == "Put" {
			kv.kvPersist[op.Key] = op.Value
		}else if op.OpType == "Append"{
			kv.kvPersist[op.Key] += op.Value
		}
		fmt.Println(kv.kvPersist)
		_, isLeader := kv.rf.GetState()
		if !isLeader{
			continue
		}
		commandIndex := msg.CommandIndex
		waitCh := kv.getWaitCh(commandIndex)
		kv.maxSeq[op.ClientId] = op.SeqId
		waitCh <- op
		fmt.Println("op已放入waitCh")
	}
}

func (kv *KVServer)ifDuplicate(clientId int64, seqId int64) bool{
	if _,exist:= kv.maxSeq[clientId];!exist{
		return false
	}
	return seqId <= kv.maxSeq[clientId]
}

func (kv *KVServer) getWaitCh(commandIndex int) chan Op{
	if _,ok := kv.waitChMap[commandIndex]; !ok{
		kv.waitChMap[commandIndex] = make(chan Op)
	}
	return kv.waitChMap[commandIndex]
}
func (kv *KVServer) getValue(key string) string{
	if value,ok := kv.kvPersist[key];ok{
		return value
	}
	return ""
}
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.kvPersist = make(map[string] string)
	kv.waitChMap = make(map[int] chan Op)
	kv.maxSeq = make(map[int64] int64)
	go kv.ApplyMsgHandlerLoop()
	return kv
}
