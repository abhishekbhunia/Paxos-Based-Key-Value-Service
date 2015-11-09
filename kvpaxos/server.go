package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"

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
	Opcode string //Get/Put/Append
	Key string
	Value string
	CurReqId string
	LastReqId string//for piggybacked client responses and deleting acked responses learned from other kvpaxos servers during (denied) agreement phases
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	seq int //for paxos agreement sequence numbers
	kvstore map[string]string
	storedReplies map[string]string
	
}

func (kv *KVPaxos) WaitForAgreement(seq int) Op{

	to := 10 * time.Millisecond
    for {
	    status, val := kv.px.Status(seq)
	    if status == paxos.Decided{
	    	return val.(Op)
	    	
	    }
	    time.Sleep(to)
	    if to < 10 * time.Second {
	      to *= 2
	    }
   }
}
func (kv *KVPaxos) CleanUp(LastReqId string) {
	if LastReqId != ""{
		_, ok := kv.storedReplies[LastReqId]
		if ok{
			delete(kv.storedReplies,LastReqId)
		}		
	}	
}
func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//clean up the old Request-Response with piggybacked/acknowledged ID
	kv.CleanUp(args.LastReqId)

	//check for duplicate request and if it's duplicate, return the stored value
	val, ok := kv.storedReplies[args.CurReqId] //Get: ErrNoKey/val Put:OK
	if ok{		
		 
		if val == ErrNoKey {
			reply.Err = ErrNoKey
			reply.Value = ""
		} else {
			reply.Value = kv.storedReplies[args.CurReqId]
			reply.Err = OK
		}
		 
		return nil
		
	}

	//else start paxos 
	for {
	 	op := Op{"Get",args.Key,"",args.CurReqId,args.LastReqId}
		seq := kv.seq
		kv.seq++
		status, val := kv.px.Status(seq)
		var ret Op
		if status == paxos.Decided{
			ret = val.(Op) //type assertion from interface to Op
		}else{
			//Opcode string, Key string, Value string, CurReqId string //GAarg Key string, CurReqId string, LastReqId string 
			kv.px.Start(seq,op)
			ret = kv.WaitForAgreement(seq)	
		}
		//the agreed instance can refer to either Get/Put/Append
		kv.UpdatePaxosLog(ret)
		kv.px.Done(seq)
		if ret.CurReqId == args.CurReqId {//if paxos actually elected this operation value then return, else try again
			 
			val := kv.storedReplies[args.CurReqId]

			if val == ErrNoKey {
				reply.Err = ErrNoKey
				reply.Value = ""
			} else {
				reply.Value = kv.storedReplies[args.CurReqId]
				reply.Err = OK
			}
			
			return nil
		}

	}

	return nil
}
func (kv *KVPaxos) UpdatePaxosLog(ret Op){
	//Opcode string, Key string, Value string, CurReqId string
	//erase the old replies as marked by the agreed instance(the old req id can be different from what was sent to Paxos and what wasreceived back
	//as Paxos might select some other operation, so old reponses has to be cleaned before and after running Paxos)
	if ret.LastReqId != ""{
		_, ok := kv.storedReplies[ret.LastReqId]
		if ok{
			delete(kv.storedReplies,ret.LastReqId)
		}
		
	}

	//for Get: val/ErrNoKey, for Put/Append: OK
	val, ok := kv.kvstore[ret.Key]
	if ret.Opcode == "Get" {		
		if ok {
			kv.storedReplies[ret.CurReqId] = val			
		} else {
			kv.storedReplies[ret.CurReqId] = ErrNoKey
		}
		return
	} else {		
		if ret.Opcode == "Put" || !ok{
	        val = "" //for Put just reset the read data; same applies for an Append with non-existing key
	    }	    
	    kv.kvstore[ret.Key] = val + ret.Value
	    kv.storedReplies[ret.CurReqId] = OK //do not store the actual value as, it may be large	    
	   	return
	}
	
}
func (kv *KVPaxos) RunPaxos(seq int,op Op) Op {
	kv.px.Start(seq,op)
	return kv.WaitForAgreement(seq)
}
func (kv *KVPaxos) StartPaxos(op Op) (int, Op){
	seq := kv.seq
    kv.seq++ 
    status, val := kv.px.Status(seq)
    var ret Op
	if status == paxos.Decided{
		ret = val.(Op) //type assertion from interface to Op
	}else{
		//Op: Opcode string, Key string, Value string, CurReqId string //PAarg Key string, Value string, Op string, CurReqId string, LastReqId string
		ret = kv.RunPaxos(seq,op)		
	}
	kv.UpdatePaxosLog(ret)
	return seq, ret
}
func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	// PutAppendArgs: Key string, Value string, Op string, CurReqId string, LastReqId string		PutAppendReply: Err Err
	//fmt.Println("Inside KVPaxos.PutAppend")
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//clean up the old Request-Response with piggybacked/acknowledged ID
	kv.CleanUp(args.LastReqId)

	//check for duplicate request and if it's duplicate, return the stored value
	_, ok := kv.storedReplies[args.CurReqId] //Get: ErrNoKey/val Put:OK
	if ok{
		//fmt.Println("Inside KVPaxos.PutAppend:Duplicate Request")
		reply.Err = OK
		return nil
	}

	//else start paxos 
	for {
		//start paxos
		//fmt.Println("Inside KVPaxos.PutAppend:Starting Paxos")		
		op := Op{args.Op,args.Key,args.Value,args.CurReqId,args.LastReqId}
		seq := kv.seq
        kv.seq++ 
		//fmt.Println("Inside KVPaxos.PutAppend:Calling Status")
		status, val := kv.px.Status(seq)
		//fmt.Println("Inside KVPaxos.PutAppend:Returning Status")
		var ret Op
		if status == paxos.Decided{
			ret = val.(Op) //type assertion from interface to Op
		}else{
			//Op: Opcode string, Key string, Value string, CurReqId string //PAarg Key string, Value string, Op string, CurReqId string, LastReqId string			 
			kv.px.Start(seq,op)//Op{args.Op,args.Key,args.Value,args.CurReqId,args.LastReqId})
			ret = kv.WaitForAgreement(seq)	//keep polling, until 'seq' agreement instance has reached consensus 
		}
		//the agreed instance can refer to either Get/Put/Append
		kv.UpdatePaxosLog(ret)
		kv.px.Done(seq)
		//end start paxos
		//fmt.Println("Inside KVPaxos.PutAppend:Done Called")
		if ret.CurReqId == args.CurReqId {//if paxos actually elected this operation value then return, else try again
			reply.Err = OK
			//fmt.Println("Inside KVPaxos.PutAppend:This operation elected")
			return nil
		}
		//fmt.Println("Inside KVPaxos.PutAppend:Repeat Paxos")
	} 

	 

	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	kv := new(KVPaxos)
	kv.me = me
	kv.seq = 0
	kv.kvstore = make(map[string]string)
    kv.storedReplies = make(map[string]string)
   

	// Your initialization code here.

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l


	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
