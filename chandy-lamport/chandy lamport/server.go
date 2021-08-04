package main

import (
	"fmt"
	"log"
)

// The main participant of the distributed snapshot protocol.
// Servers exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one server to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.
type Server struct {
	Id            string
	Tokens        int
	sim           *Simulator
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src
	// TODO: ADD MORE FIELDS HERE
	bros      int
	marks     map[string]int
	lastmark  int
	myactives map[int]*activesnapsgot
	buffered  map[int]bool
}

// A unidirectional communication channel between two servers
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src    string
	dest   string
	events *Queue
}

func NewServer(id string, tokens int, sim *Simulator) *Server {
	return &Server{
		id,
		tokens,
		sim,
		make(map[string]*Link),
		make(map[string]*Link),
		0,
		make(map[string]int),
		-1,
		make(map[int]*activesnapsgot),
		make(map[int]bool)}
}
func (server *Server) newacctivesnapshot(id int, frombro bool) bool {
	server.myactives[id] = &activesnapsgot{server, server.bros, 0, server.Tokens, make([]SnapshotMessage, 0)}
	if !frombro {
		return false
	}
	return (*(server.myactives[id])).addmarker()

}
func (server *Server) snaprec(src string, message TokenMessage) {
	nextavtive, f := server.marks[src]

	if !f {
		nextavtive = 0
	} else {
		nextavtive++
	}
	for {
		if nextavtive > server.lastmark {
			break
		}
		_, ok := server.myactives[nextavtive]
		if !ok {
			continue
		}
		(*((*server).myactives[nextavtive])).addtokedmeeage(src, message)
		nextavtive++
	}

}

// Add a unidirectional link to the destination server
func (server *Server) AddOutboundLink(dest *Server) {
	if server == dest {
		return
	}
	dest.bros++

	l := Link{server.Id, dest.Id, NewQueue()}
	server.outboundLinks[dest.Id] = &l
	dest.inboundLinks[server.Id] = &l
}

// Send a message on all of the server's outbound links
func (server *Server) SendToNeighbors(message interface{}) {
	for _, serverId := range getSortedKeys(server.outboundLinks) {
		link := server.outboundLinks[serverId]
		server.sim.logger.RecordEvent(
			server,
			SentMessageEvent{server.Id, link.dest, message})
		link.events.Push(SendMessageEvent{
			server.Id,
			link.dest,
			message,
			server.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this server
func (server *Server) SendTokens(numTokens int, dest string) {
	if server.Tokens < numTokens {
		log.Fatalf("Server %v attempted to send %v tokens when it only has %v\n",
			server.Id, numTokens, server.Tokens)
	}
	message := TokenMessage{numTokens}
	server.sim.logger.RecordEvent(server, SentMessageEvent{server.Id, dest, message})
	// Update local state before sending the tokens
	server.Tokens -= numTokens
	link, ok := server.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from server %v\n", dest, server.Id)
	}
	link.events.Push(SendMessageEvent{
		server.Id,
		dest,
		message,
		server.sim.GetReceiveTime()})
}

// Callback for when a message is received on this server.
// When the snapshot algorithm completes on this server, this function
// should notify the simulator by calling `sim.NotifySnapshotComplete`.
func (server *Server) HandlePacket(src string, message interface{}) {
	/*defer{
		recover()
	}*/
	// TODO: IMPLEMENT ME
	switch msg := message.(type) {
	case TokenMessage:
		server.Tokens += msg.numTokens
		(*server).snaprec(src, msg)
	case MarkerMessage:
		server.marks[src] = msg.snapshotId
		_, ok := server.myactives[msg.snapshotId]
		if !ok {

			(*server).StartSnapshot(msg.snapshotId, true)

		} else {
			if (*(server.myactives[msg.snapshotId])).addmarker() {
				server.sim.NotifySnapshotComplete(server.Id, msg.snapshotId)
			}
		}
	}
	fmt.Sprintf("Unrecognized message: %v", message)
}

// Start the chandy-lamport snapshot algorithm on this server.
// This should be called only once per server.
func free(s *Server, snapshotid int) {
	_, ok := (*s).buffered[snapshotid+1]
	if ok {
		(*s).StartSnapshot(snapshotid+1, false)
	}
}
func (server *Server) StartSnapshot(snapshotId int, frombro bool) {
	// TODO: IMPLEMENT ME
	if snapshotId > (server.lastmark + 1) {
		if frombro {
			fmt.Println()
		}
		server.buffered[snapshotId] = true
		return
	}
	if server.lastmark != snapshotId-1 {
		fmt.Println()
	}
	server.lastmark = snapshotId
	if (*server).newacctivesnapshot(snapshotId, frombro) {
		(*server.sim).NotifySnapshotComplete(server.Id, snapshotId)
	}
	(*server).SendToNeighbors(MarkerMessage{snapshotId})
	free(server, snapshotId)

}

type activesnapsgot struct {
	myserver      *Server
	currentbro    int
	finishedbros  int
	myservertoken int
	messages      []SnapshotMessage
}

func (active *activesnapsgot) addmarker() bool {
	active.finishedbros++
	if active.finishedbros == active.currentbro {
		return true
	} else {
		return false
	}

}
func (active *activesnapsgot) addtokedmeeage(src string, message TokenMessage) {
	active.messages = append(active.messages, SnapshotMessage{src, active.myserver.Id, message})
}
