package kvraft

import "fmt"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrOutTime = "ErrOutTime"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	OpType   string // "Put" or "Append"
	ClientId int64
	SeqId    int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	SeqId int64
}

type GetReply struct {
	Err   Err
	Value string
}

func (args PutAppendArgs)String()  string{
	return fmt.Sprintf("PutAppendArgs{Key:%s,Value:%s,OpType:%v,ClientId:%d,SeqId:%d}",
		args.Key,args.Value,args.OpType,args.ClientId,args.SeqId)
}
func (args GetArgs)String()  string{
	return fmt.Sprintf("GetArgs{Key:%s,ClientId:%d,SeqId:%d}",
		args.Key,args.ClientId,args.SeqId)
}