package shardctrler

//
// Shardctrler clerk.
//

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId  int64
	commandId int64
	leaderId  int64
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
	ck.clientId = nrand()
	ck.commandId = 0
	ck.leaderId = 0
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &CommandRequest{
		Op:        OpQuery,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
		Num:       num,
	}
	return ck.Command(args)
	// Your code here.

	//for {
	//	// try each known server.
	//	for _, srv := range ck.servers {
	//		var reply CommandReply
	//		ok := srv.Call("ShardCtrler.Query", args, &reply)
	//		if ok && reply.WrongLeader == false {
	//			ck.CommandId++
	//			return reply.Config
	//		}
	//	}
	//	time.Sleep(100 * time.Millisecond)
	//}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &CommandRequest{
		Op:        OpJoin,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
		Servers:   servers,
	}
	ck.Command(args)
	// Your code here.
	//args.Servers = servers

	//for {
	//	// try each known server.
	//	for _, srv := range ck.servers {
	//		var reply JoinReply
	//		ok := srv.Call("ShardCtrler.Join", args, &reply)
	//		if ok && reply.WrongLeader == false {
	//			return
	//		}
	//	}
	//	time.Sleep(100 * time.Millisecond)
	//}
}

func (ck *Clerk) Leave(gids []int) {
	args := &CommandRequest{
		Op:        OpLeave,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
		GIDs:      gids,
	}
	ck.Command(args)
	// Your code here.
	//args.GIDs = gids
	//
	//for {
	//	// try each known server.
	//	for _, srv := range ck.servers {
	//		var reply LeaveReply
	//		ok := srv.Call("ShardCtrler.Leave", args, &reply)
	//		if ok && reply.WrongLeader == false {
	//			return
	//		}
	//	}
	//	time.Sleep(100 * time.Millisecond)
	//}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &CommandRequest{
		Op:        OpMove,
		ClientId:  ck.clientId,
		CommandId: ck.commandId,
		Shard:     shard,
		GID:       gid,
	}
	ck.Command(args)
	// Your code here.
	//args.Shard = shard
	//args.GID = gid
	//
	//for {
	//	// try each known server.
	//	for _, srv := range ck.servers {
	//		var reply MoveReply
	//		ok := srv.Call("ShardCtrler.Move", args, &reply)
	//		if ok && reply.WrongLeader == false {
	//			return
	//		}
	//	}
	//	time.Sleep(100 * time.Millisecond)
	//}
}

func (ck *Clerk) Command(request *CommandRequest) Config {
	request.ClientId, request.CommandId = ck.clientId, ck.commandId
	for {
		DPrintf("client %v send command %v.", ck.clientId, ck.commandId)
		var reply CommandReply
		if !ck.servers[ck.leaderId].Call("ShardCtrler.Command", request, &reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			continue
		}
		ck.commandId++
		return reply.Config
	}
}
