package paxos
import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"
type DoneArgs struct{
	Me			int
	DoneID		int
}

type DoneReply struct{
	OK	bool
}
type DecidedArgs struct{
	Seq int
	Val interface{}
}
type DecidedReply struct{
	Commit bool
}
type PrepareArgs struct{
	Seq int
	PropNum int	
}
type PrepareReply struct {
	Status Fate
	N_A int
	V_A interface{}
}
type AcceptArgs struct {
	Seq int
	PropNum int
	PropVal interface{}	
}
type AcceptReply struct {
	Status bool
}
type Fate int

/*const (
	Decided = "Decided"
	Promised = "Promised"		// promised by acceptor, when proposal # > highest proposal seen
	Rejected = "Rejected"		// rejectedby acceptor, when proposal # < highest proposal seen
	Pending = "Pending"       // not yet decided.
	Forgotten = "Forgotten"    // decided but forgotten.
)*/

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
	Promised
	Rejected
)
//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//




// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).

type PeerState struct{ //for recording state of the peer for every agreement instance	
	n_p	int //max seen proposal number
	n_a	int //max accepted proposal number
	v_a	interface{} //max accepted proposal value
	propNum	int //stores proposal number in each iteration
	decided	bool //marks the state of the current agreement instance
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]


	// Your data here.
	agreementStates map[int]*PeerState //stores the paxos state for all agreement sequences for this peer
	doneMap		[]int
}



// the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.

func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
func (px *Paxos) PrepareHandler(args *PrepareArgs,reply *PrepareReply) error{
	px.mu.Lock()
	//check if node has seen the agreement #
	agreement, ok := px.agreementStates[args.Seq]
	if ok{// if it has seen it, promise it(send prepare_ok) and send stored accpeted proposal # & val
		
		if px.agreementStates[args.Seq].n_p < args.PropNum{
			reply.Status = Promised 
			reply.N_A = agreement.n_a
			reply.V_A = agreement.v_a
			px.agreementStates[args.Seq].n_p = args.PropNum
		}

	}else{ //else just create a record and promise
			reply.Status = Promised 
			reply.N_A = -1 
			reply.V_A = nil
			px.agreementStates[args.Seq] = &PeerState{args.PropNum,-1,nil,px.me,false}
			
	}
	px.mu.Unlock()	
	return nil
}
func (px *Paxos) PrepareRequest(seq int, propNum int, peerStr string) PrepareReply{ //PrepareReply:Status Fate,N_A int,V_A interface{}
	//PrepareArgs:Seq int,PropNum int
	var reply PrepareReply
	
	ok := call(peerStr, "Paxos.PrepareHandler", &PrepareArgs{seq,propNum}, &reply)
	
	if ok {
		return reply
	}

	return PrepareReply{Rejected, -1,  nil}
}
func (px *Paxos) AcceptRequest(seq int, propNum int, propVal interface{}, peerStr string) AcceptReply{
	// AcceptArgs: Seq int, PropNum int, PropVal interface{} AcceptReply: Status bool
	var reply AcceptReply
	ok := call(peerStr, "Paxos.AcceptHandler", &AcceptArgs{seq, propNum, propVal}, &reply) 

	if ok && reply.Status{
		return AcceptReply{true}
	}

	return AcceptReply{false}
}
func (px *Paxos) AcceptHandler(args *AcceptArgs, reply *AcceptReply) error{
	 px.mu.Lock()
	 //check if node has seen the agreement #
	_, ok := px.agreementStates[args.Seq]	
	reply.Status = true
	if ok { //if it has seen it and poposal no>stored proposal # then accept_ok else reject
		if args.PropNum >= px.agreementStates[args.Seq].n_p{
			px.agreementStates[args.Seq].n_p = args.PropNum
			px.agreementStates[args.Seq].n_a = args.PropNum
			px.agreementStates[args.Seq].v_a = args.PropVal

		}else{
			reply.Status = false
		}
	}else{
		px.agreementStates[args.Seq] = &PeerState{args.PropNum,args.PropNum,args.PropVal,px.me,false}
	}

	px.mu.Unlock()

	return nil 
	
}


func (px *Paxos) Decided(seq int, propVal interface{}) {
	//DecidedArgs: Seq int,	Val interface{} DecidedReply: Commit bool	 
	var reply DecidedReply
	for i:=0; i<len(px.peers); i++ {
		if i!=px.me {
			call(px.peers[i], "Paxos.DecidedHandler", &DecidedArgs{seq,propVal}, &reply)
		}
	}

}
func (px *Paxos) DecidedHandler(args *DecidedArgs, reply *DecidedReply) error{
	px.mu.Lock()
	reply.Commit = true
	_, ok := px.agreementStates[args.Seq]
	//if the peer has seen the agreement instance, just update the record, else create one and mark it as decided
	if ok{
		px.agreementStates[args.Seq].decided = true
		px.agreementStates[args.Seq].v_a = args.Val

	}else{
		px.agreementStates[args.Seq] = &PeerState{-1,-1,args.Val,px.me,true}
	}
	px.mu.Unlock()
	return nil
}
//Accept(propNum,propVal,px.peers[i],seq,peers[me])

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) getPropNum(seq int, prep_ok_max_p int) int {
	px.mu.Lock()
	for px.agreementStates[seq].propNum <= prep_ok_max_p{		
		delta := prep_ok_max_p - px.agreementStates[seq].propNum
		px.agreementStates[seq].propNum = px.agreementStates[seq].propNum + delta + 1
	} 
	ret := px.agreementStates[seq].propNum
	px.agreementStates[seq].propNum += 1
	px.mu.Unlock()
	return ret
}
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	px.agreementStates[seq]=&PeerState{-1,-1,nil,px.me,false}
	go func(seq int, val interface{}){
		decided := false
		maxPropNum := -1
		var maxPropVal interface{} = nil
		firstToPropose := true
		for !px.isdead() && !decided{
			majority := 0	
			propNum := px.getPropNum(seq,maxPropNum)
			
			//prepare phase
			//send prepare to all and count prepare_ok
			for i := 0; i < len(px.peers); i ++{
				prepareReply := px.PrepareRequest(seq, propNum, px.peers[i]) ////PrepareReply:Status Fate,N_A int,V_A interface{}
				
				//when Promised, remember the highest proposal number seen by the other node and it's corresponding value 
				if prepareReply.Status == Promised{
					if prepareReply.N_A > maxPropNum{
						maxPropNum = prepareReply.N_A
						maxPropVal = prepareReply.V_A
						firstToPropose = false
					}
					majority += 1
				}				
			}
			//accept: begin accept phase if majority promised
			if !decided && majority > len(px.peers)/2{ //if prepare_ok received from majority
				majority = 0 //start counting accept_ok
				
				if !firstToPropose{
					val = maxPropVal
				}
				//send accept request to everyone and count accept_ok
				for i := 0; i < len(px.peers); i ++{
					ok := px.AcceptRequest(seq, propNum, val, px.peers[i]) //AcceptReply: Status bool
					if ok.Status{ //count accept_ok
						majority += 1
					}
				}
				//if accept_ok received from majority, send decided to all
				if majority > len(px.peers)/2{ //if accept_ok received from majority, mark the sequence as decided and store the decided value
					px.agreementStates[seq].decided = true
					px.agreementStates[seq].v_a = val
					px.Decided(seq, val)
					decided = true //break out of the loop
				}
				//decided = px.agreementStates[seq].decided
			}
		}

	}(seq,v)
	
}
	



//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//

func (px *Paxos) Done(seq int) {
	// Your code here.
	// nothing to do if passed agreement sequence number is a stale value
	if seq <= px.doneMap[px.me] {
		return
	}
	//else update done value for itself and call Min() locally, then
	px.doneMap[px.me] = seq
	
	//send RPC calls to all other peers notifying updated self done value so that they can call their Min() 
	var reply DoneReply
	for i:=0; i<len(px.peers); i++ {
		if i!=px.me {
			call(px.peers[i], "Paxos.DoneHandler", &DoneArgs{px.me,seq}, &reply)
		}
	}
	px.Min()

}

func (px *Paxos) DoneHandler(args *DoneArgs, reply *DoneReply) error{
	//for the local done state of sender peer, update the done value only if it corresponds to a newer agreement sequence
	if args.DoneID > px.doneMap[args.Me] {
		px.doneMap[args.Me] = args.DoneID
		px.Min()
	}

	return nil
}
//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	max := -1
	//iterate through the agreement state map to uncover the highest agreement seq seen by the peer and return it
	for key, _ := range px.agreementStates{
		if key > max{
			max = key
		}
	}
	return max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	
	px.mu.Lock()

	//calculate minimum done value(done agreement #) among all peers
	minVal := px.doneMap[0]
	for i:=0; i<len(px.doneMap); i++ {
		if px.doneMap[i]<minVal {
			minVal = px.doneMap[i]
		}
	}
	//now iterate the agreement map with agreement #<minVal and reset if that agreement has been decided
	for i:=0; i<=minVal; i++ {
		agreement, ok := px.agreementStates[i]
		if ok {
			if agreement.decided{
				px.agreementStates[i].v_a = nil
			}			
		}
	}
	px.mu.Unlock()
	return  minVal+1

}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// return forgotten if seq is less than min+1 

	if seq < px.Min(){
		return Forgotten, nil
	}
	px.mu.Lock()
	// else return the status and acceted vanue(if the agreement has been decided)
	agreement, ok := px.agreementStates[seq]
	if ok{
		if agreement.decided{
			px.mu.Unlock()
			return Decided, agreement.v_a 
		}
	}
	px.mu.Unlock()
	return Pending, nil

}



//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	
	px := &Paxos{}
	px.peers = peers
	px.me = me
	px.mu = sync.Mutex{}

	// Your initialization code here.
	px.doneMap  = make([]int, len(peers))
	for i := 0; i<len(px.peers); i++{
		px.doneMap[i] = -1
	}
	px.agreementStates = make(map[int]*PeerState)
	 

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}


	return px
}
