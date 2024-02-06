package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.5840/labrpc"
)
import "crypto/rand"
import "math/big"
import "6.5840/shardctrler"
import "time"

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm      *shardctrler.Clerk
	config  shardctrler.Config             // the latest config which the clerk has read
	makeEnd func(string) *labrpc.ClientEnd // convert from serverName to clinetEnd handle
	// You will have to modify this struct.

	leaderIds map[int]int // 不同分片的leader
	clientId  int64       // random value to judge between clients
	commandId int64       // (clientId, commandId) to define a command uniquely
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, makeEnd func(string) *labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		sm:        shardctrler.MakeClerk(ctrlers),
		makeEnd:   makeEnd, // from serverName to clinetEnd handle
		leaderIds: make(map[int]int),
		clientId:  nrand(),
		commandId: 0,
	}
	ck.config = ck.sm.Query(-1) // init the clerk config
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	return ck.Command(&CommandRequest{Key: key, Op: OpGet})
	//args := GetArgs{}
	//args.Key = key
	//
	//for {
	//	shard := key2shard(key)
	//	gid := ck.config.Shards[shard]
	//	if servers, ok := ck.config.Groups[gid]; ok {
	//		// try each server for the shard.
	//		for si := 0; si < len(servers); si++ {
	//			srv := ck.makeEnd(servers[si])
	//			var reply GetReply
	//			ok := srv.Call("ShardKV.Get", &args, &reply)
	//			if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
	//				return reply.Value
	//			}
	//			if ok && (reply.Err == ErrWrongGroup) {
	//				break
	//			}
	//			// ... not ok, or ErrWrongLeader
	//		}
	//	}
	//	time.Sleep(100 * time.Millisecond)
	//	// ask controler for the latest configuration.
	//	ck.config = ck.sm.Query(-1)
	//}
	//
	//return ""
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{}
	args.Key = key
	args.Value = value
	args.Op = op

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.makeEnd(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	//ck.PutAppend(key, value, "Put")
	ck.Command(&CommandRequest{Key: key, Value: value, Op: OpPut})
}

func (ck *Clerk) Append(key string, value string) {
	//ck.PutAppend(key, value, "Append")
	ck.Command(&CommandRequest{Key: key, Value: value, Op: OpAppend})
}

func (ck *Clerk) Command(request *CommandRequest) string {
	request.ClientId, request.CommandId = ck.clientId, ck.commandId
	// 组间循环
	for {
		shard := key2shard(request.Key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// invalid the shard of the wrong group
			if _, ok = ck.leaderIds[gid]; !ok {
				ck.leaderIds[gid] = 0
			}
			// 不能无限请求同一组内的服务器，请求一圈后初步判定该服务器组不负责本分片
			oldLeaderId := ck.leaderIds[gid]
			newLeaderId := oldLeaderId
			// 组内循环
			for {
				var reply *CommandReply
				ok := ck.makeEnd(servers[newLeaderId]).Call("ShardKV.Command", request, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.commandId++
					return reply.Value
				} else if ok && reply.Err == ErrWrongGroup {
					break
				} else {
					newLeaderId = (newLeaderId + 1) % len(servers)
					if newLeaderId == oldLeaderId {
						break
					}
					continue
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// 指定组内请求失败，获取最新配置
		ck.config = ck.sm.Query(-1)
	}
}
