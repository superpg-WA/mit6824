package raft

import "log"

// Debugging
const Debug = false

const FOLLOWER = 1
const CANDIDATE = 2
const LEADER = 3

// headbeat: no more then 10 times a second
const HEARTBEAT_DURATION = 100

// election timeouts start a election   bigger than 150 - 300
const ELECTION_TIMEOUT = 200

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
