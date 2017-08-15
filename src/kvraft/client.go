package raftkv

import "labrpc"
import "crypto/rand"
import (
	"math/big"
	"sync"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id int64
	serNum int
	mu      sync.Mutex
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
	ck.id =nrand()
	ck.serNum = 0
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	var args GetArgs
	args.Key = key
	args.ClientId = ck.id
	ck.mu.Lock()
	args.SerNum = ck.serNum
	ck.serNum++
	ck.mu.Unlock()

	for {
		for _, server := range ck.servers {
			var reply GetReply
			ok := server.Call("RaftKV.Get", &args, &reply)

			if(ok && !reply.WrongLeader) {
				//println("Get", key)
				return reply.Value
			}

		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	var args PutAppendArgs
	args.ClientId = ck.id
	args.Op = op
	args.Key = key
	args.Value = value
	ck.mu.Lock()
	args.SerNum = ck.serNum
	ck.serNum++
	ck.mu.Unlock()

	for {
		for _, server := range ck.servers {
			var reply PutAppendReply
			ok := server.Call("RaftKV.PutAppend", &args, &reply)

			if(ok && !reply.WrongLeader) {
				//println(op, "Key:", key, "Value:", value)
				return
			}

		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
