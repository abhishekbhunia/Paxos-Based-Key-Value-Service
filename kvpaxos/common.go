package kvpaxos
import "time"
const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string
const PingInterval = time.Millisecond * 100
// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CurReqId string
	LastReqId string // for piggybacked ack, need this to clear the cache
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	CurReqId string
	LastReqId string // for piggybacked ack, need this to clear the cache
}

type GetReply struct {
	Err   Err
	Value string
}
