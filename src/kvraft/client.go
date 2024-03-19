package kvraft

import (
	"6.5840/labrpc"
	"fmt"
)
import "crypto/rand"
import "math/big"
import mathrand "math/rand"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	seqId int64
	leaderId int
	clientId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.leaderId = mathrand.Intn(len(ck.servers))
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.seqId++
	args := GetArgs{Key: key,SeqId:ck.seqId,ClientId:ck.clientId}
	reply := GetReply{}
	for{
		fmt.Println("key:",key)
		fmt.Printf("client[%d]向server[%d]发送%v \n",ck.clientId,ck.leaderId,args)
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if ok{
			if reply.Err == OK{
				fmt.Printf("client[%d]   OK:%v \n",ck.clientId,reply.Value)
				return reply.Value
			}else if reply.Err == ErrNoKey{
				fmt.Printf("client[%d]   NoKey \n",ck.clientId)
				return ""
			}
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	}

}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.seqId++
	args := PutAppendArgs{Key:key,Value:value, OpType:op,SeqId:ck.seqId,ClientId:ck.clientId}
	reply := PutAppendReply{}
	for{
		fmt.Printf("client[%d]向server[%d]发送%v \n",ck.clientId,ck.leaderId,args)
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if ok  && reply.Err == OK{
			fmt.Printf("client[%d]   PutAppend:%v \n",ck.clientId,args)
			return
		}
		ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
