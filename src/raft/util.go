package raft

import "log"

// Debugging
const Debug = false

const FOLLOWER = 1
const CANDIDATE = 2
const LEADER = 3

// headbeat: no more then 10 times a second
const HEARTBEAT_DURATION int64 = 50

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func IntegerMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func IntegerMax(a, b int) int {
	if a > b {
		return a
	}
	return b
}
