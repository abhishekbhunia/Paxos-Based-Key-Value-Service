package kvpaxos

import "net/rpc"
import "crypto/rand"
import "math/big"
import "strconv"
import "time"
import "fmt"

type Clerk struct {
	servers []string
	// You will have to modify this struct.
	//use nrand to generate random numbers for client IDs and requestIDs for the kvpaxos servers to identify and manage different requests
	clerkID string 
	reqID string
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkID = strconv.FormatInt(nrand(), 10)
	ck.reqID = "" //pair<clerkID,tandom number for the request>; semantics: previous operation ID, so that it can be piggybacked 
					//as an ack in the next call and kvpaxos can delete the response cache
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	// GetArgs: Key string, CurReqId string, LastReqId string	GetReply: Err Err, Value string
	var reply GetReply
	newReqID := ck.clerkID + "," + strconv.FormatInt(nrand(), 10)
	 
	for i:=0;;i++{
		ok := call(ck.servers[(i)%len(ck.servers)], "KVPaxos.Get", &GetArgs{key,newReqID,ck.reqID}, &reply) //loop endless until a response is heard
        if ok{
            ck.reqID = newReqID //store the ID to be sent as a piggybacked value so that kvserver can delete served requests in future calls
            return reply.Value
        }
        time.Sleep(PingInterval) //sleep before trying a new server
         
	}
	return ""
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//PutAppendArgs: Key string, Value string, Op string, CurReqId string, LastReqId string		PutAppendReply: Err Err
	//fmt.Println("Inside Clerk PutAppend")
	var reply PutAppendReply
	newReqID := ck.clerkID + "," + strconv.FormatInt(nrand(), 10)

	for i:=0;;i++{
		//fmt.Println("Calling KVPaxos.PutAppend")
        ok := call(ck.servers[i], "KVPaxos.PutAppend", &PutAppendArgs{key,value,op,newReqID,ck.reqID}, &reply)        
        if ok{
        	//fmt.Println("Success KVPaxos.PutAppend")
            ck.reqID = newReqID //store the ID to be sent as a piggybacked value so that kvserver can delete served requests in future calls
            return
        }
        time.Sleep(PingInterval)        
    }
    //fmt.Println("Exiting Clerk PutAppend")
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
