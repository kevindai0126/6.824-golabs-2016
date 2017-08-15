package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Operator string
	Key string
	Value string
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db	map[string]string
	result	map[int]chan Op
}

func (kv *RaftKV) ReplicateLog(entry Op) bool {
	index, _, isLeader := kv.rf.Start(entry)


	if (!isLeader) {
		return false
	} else {
		kv.mu.Lock()
		ch, ok := kv.result[index]
		if (!ok) {
			ch = make(chan Op, 1)
			kv.result[index] = ch
		}
		kv.mu.Unlock()

		select {
		case op := <-ch:
			return op == entry
		case <-time.After(1000 * time.Millisecond):
			log.Printf("timeout\n")
			return false
		}
		return true
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	entry := Op{Operator:"Get",Key:args.Key}

	ok := kv.ReplicateLog(entry)

	if ok {
		reply.WrongLeader = false
		reply.Err = OK
		kv.mu.Lock()
		reply.Value = kv.db[args.Key]
		kv.mu.Unlock()
	} else {
		reply.WrongLeader = true
	}

}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	entry := Op{Operator:args.Op, Key:args.Key, Value:args.Value}

	ok := kv.ReplicateLog(entry)

	if ok {
		reply.WrongLeader = false
		reply.Err = OK
		//println(args.Op, "Key:", args.Key, "Value:", args.Value)
	} else {
		reply.WrongLeader = true
	}
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.
	kv.db = make(map[string]string)
	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.result = make(map[int]chan Op)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)


	go func() {
		for {
			msg := <-kv.applyCh
			op := msg.Command.(Op)

			kv.mu.Lock()
			switch op.Operator {
			case "Put":
				kv.db[op.Key] = op.Value
			case "Append":
				kv.db[op.Key] += op.Value
			}
			ch, ok := kv.result[msg.Index]
			if !ok {
				kv.result[msg.Index] = make(chan Op, 1)
			} else {
				ch <- op
			}
			kv.mu.Unlock()
		}
	}()

	return kv
}
