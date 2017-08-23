package shardmaster


import "raft"
import "labrpc"
import "sync"
import (
	"encoding/gob"
	"time"
	"fmt"
)


type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config // indexed by config num
	result	map[int]chan Op
	ack 	map[int64]int
}


type Op struct {
	// Your data here.
	Operator      string
	Num     int
	GIDs    []int
	Servers map[int][]string
	Shard   int
	ClientId int64
	SerNum int
}

func (sm *ShardMaster) isDedup(clientId int64, serNum int) bool {
	sm.mu.Lock()
	latestNum, ok := sm.ack[clientId]
	sm.mu.Unlock()

	if(ok && serNum <= latestNum) {
		return true
	} else {
		return false
	}
}

func (sm *ShardMaster) ReplicateLog(entry Op) bool {
	index, _, isLeader := sm.rf.Start(entry)


	if (!isLeader) {
		return false
	} else {
		sm.mu.Lock()
		ch, ok := sm.result[index]
		if (!ok) {
			ch = make(chan Op, 1)
			sm.result[index] = ch
		}
		sm.mu.Unlock()

		select {
		case op := <-ch:
			return op.SerNum == entry.SerNum
		case <-time.After(1000 * time.Millisecond):
		//log.Printf("timeout\n")
			return false
		}
	}
}

func (sm *ShardMaster) getLatestConfig() Config {
	return sm.configs[len(sm.configs) - 1]
}

func (sm *ShardMaster) getLatestConfigReference() *Config {
	return &sm.configs[len(sm.configs) - 1]
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if(sm.isDedup(args.ClientId, args.SerNum)) {
		reply.WrongLeader = false
		reply.Err = OK
	} else {

		entry := Op{Operator:"Join", Servers:args.Servers, ClientId:args.ClientId, SerNum:args.SerNum}

		ok := sm.ReplicateLog(entry)

		if ok {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if(sm.isDedup(args.ClientId, args.SerNum)) {
		reply.WrongLeader = false
		reply.Err = OK
	} else {

		entry := Op{Operator:"Leave", GIDs:args.GIDs, ClientId:args.ClientId, SerNum:args.SerNum}

		ok := sm.ReplicateLog(entry)

		if ok {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	if(sm.isDedup(args.ClientId, args.SerNum)) {
		reply.WrongLeader = false
		reply.Err = OK
	} else {

		entry := Op{Operator:"Move", Shard:args.Shard, GIDs: append(make([]int, 0),args.GID), ClientId:args.ClientId, SerNum:args.SerNum}

		ok := sm.ReplicateLog(entry)

		if ok {
			reply.WrongLeader = false
			reply.Err = OK
		} else {
			reply.WrongLeader = true
		}
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if(sm.isDedup(args.ClientId, args.SerNum)) {
		if(args.Num == -1 || args.Num >= len(sm.configs)) {
			reply.Config = sm.getLatestConfig()
		} else {
			reply.Config = sm.configs[args.Num]
		}
	} else {

		entry := Op{Operator:"Query", Num:args.Num, ClientId:args.ClientId, SerNum:args.SerNum}

		ok := sm.ReplicateLog(entry)

		if ok {
			reply.WrongLeader = false
			reply.Err = OK
			sm.mu.Lock()
			if(args.Num == -1 || args.Num >= len(sm.configs)) {
				reply.Config = sm.getLatestConfig()
			} else {
				reply.Config = sm.configs[args.Num]
			}
			sm.ack[args.ClientId] = args.SerNum
			sm.mu.Unlock()
		} else {
			reply.WrongLeader = true
		}
	}
}

func (sm *ShardMaster) newConfig() *Config {
	old := &sm.configs[len(sm.configs) - 1]
	new := Config{}
	new.Groups = map[int][]string{}
	new.Num = old.Num + 1
	new.Shards = [NShards]int{}
	for gid, servers := range old.Groups {
		new.Groups[gid] = servers
	}
	for i, gid := range old.Shards {
		new.Shards[i] = gid
	}
	sm.configs = append(sm.configs, new)
	return &sm.configs[len(sm.configs) - 1]

}

func (sm *ShardMaster) showConfig(config Config) {
	println("Server", sm.me)
	println("Num", config.Num)
	println("Groups")
	for gid, servers := range config.Groups {
		println("GID", gid)
		fmt.Printf("%v\n", servers)
	}
	fmt.Printf("Shards:%v]n", config.Shards)
}

func (sm *ShardMaster) getOneShardByGID(gid int) int {
	config := sm.getLatestConfigReference()

	for shard, id := range config.Shards {
		if id == gid {
			return shard
		}
	}

	return -1
}

func (sm *ShardMaster) getGIDWithMostShard() int {
	config := sm.getLatestConfigReference()

	//There is unassigned shard
	for _, gid := range config.Shards {
		if gid == 0 {
			return 0
		}
	}

	count := map[int]int{}
	max := -1
	result := 0

	for gid := range config.Groups {
		count[gid] = 0
	}

	for _, gid := range config.Shards {
		count[gid]++
	}

	for gid, c := range count {
		_, ok := config.Groups[gid]
		if ok && c > max {
			max, result = c, gid
		}
	}

	return result
}

func (sm *ShardMaster) getGIDWithLeastShard() int {
	config := sm.getLatestConfigReference()

	//There is unassigned shard
	for _, gid := range config.Shards {
		if gid == 0 {
			return 0
		}
	}

	count := map[int]int{}
	min := len(config.Shards)
	result := 0

	for gid := range config.Groups {
		count[gid] = 0
	}

	for _, gid := range config.Shards {
		count[gid]++
	}

	for gid, c := range count {
		_, ok := config.Groups[gid]
		if ok && c < min {
			min, result = c, gid
		}
	}

	return result
}

func (sm *ShardMaster) rebalanceAfterJoin(gid int) {
	config := sm.getLatestConfigReference()
	num := NShards / len(config.Groups)

	for i := 0; i < num; i++ {
		maxGid := sm.getGIDWithMostShard()
		shard := sm.getOneShardByGID(maxGid)
		config.Shards[shard] = gid
	}
}

func (sm *ShardMaster) rebalanceAfterLeave(gid int) {
	config := sm.getLatestConfigReference()

	for shard, id := range config.Shards {
		if (id == gid) {
			minGid := sm.getGIDWithLeastShard()
			config.Shards[shard] = minGid
		}
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.result = make(map[int]chan Op)
	sm.ack = make(map[int64]int)

	// Your code here.
	go func() {
		for {
			msg := <-sm.applyCh
			if msg.UseSnapshot {
			} else {
				op := msg.Command.(Op)

				if(!sm.isDedup(op.ClientId, op.SerNum)) {
					sm.mu.Lock()
					switch op.Operator {
					case "Join":
						config := sm.newConfig()
						for gid, servers := range op.Servers {
							_, ok := config.Groups[gid]
							if !ok {
								config.Groups[gid] = servers
								sm.rebalanceAfterJoin(gid)
							}
						}

						//sm.showConfig(sm.getLatestConfig())
					case "Leave":
						config := sm.newConfig()
						for _, gid := range op.GIDs {
							_, ok := config.Groups[gid]
							if ok {
								delete(config.Groups, gid)
								sm.rebalanceAfterLeave(gid)
							}
						}
					case "Move":
						config := sm.newConfig()
						if op.GIDs != nil && len(op.GIDs) > 0 {
							config.Shards[op.Shard] = op.GIDs[0]
						}
					}
					sm.ack[op.ClientId] = op.SerNum
					sm.mu.Unlock()
				}
				sm.mu.Lock()

				ch, ok := sm.result[msg.Index]
				if !ok {
					sm.result[msg.Index] = make(chan Op, 1)
				} else {
					select {
					case <-ch:
					default:
					}
					ch <- op
				}

				sm.mu.Unlock()
			}
		}
	}()

	return sm
}
