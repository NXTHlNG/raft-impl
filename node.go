package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"
)

const (
	HeartBeat          = 1000
	ElectionMinTimeout = 5000
	ElectionMaxTimeout = 10000
)

type Node struct {
	name                    string
	port                    string
	currentTerm             int
	votedFor                string
	votesCount              int
	commitLength            int
	logs                    []string
	currentRole             string
	leaderNodeId            string
	electionTimeout         *time.Ticker
	resetElectionTimer      chan struct{}
	electionTimeoutInterval int
	//ports                   []string
	cluster *Cluster
}

func NewNode(name string, port string, cluster *Cluster) *Node {
	rand.New(rand.NewSource(time.Now().UnixNano()))
	electionTimeoutInterval := rand.Intn(int(ElectionMaxTimeout)-int(ElectionMinTimeout)) + int(ElectionMinTimeout)

	node := &Node{
		name: name,
		port: port,
		//ports: ports,
		//currentRole:             "follower",
		electionTimeout:         time.NewTicker(time.Duration(electionTimeoutInterval) * time.Millisecond),
		resetElectionTimer:      make(chan struct{}),
		electionTimeoutInterval: electionTimeoutInterval,
		cluster:                 cluster,
	}

	return node
}

type Message struct {
	Key  string `json:"key"`
	Data any    `json:"data"`
}

type VoteResponse struct {
	NodeId      string `json:"nodeId"`
	CurrentTerm int    `json:"currentTerm"`
	Accepted    bool   `json:"accepted"`
}

func (n *Node) handleConnection(c net.Conn) {
	defer c.Close()

	for {
		if n.currentRole == "dead" {
			continue
		}

		decoder := json.NewDecoder(c)

		var message Message

		err := decoder.Decode(&message)
		if err != nil {
			log.Fatal(err.Error())
		}

		var response *Message

		switch message.Key {
		case "vote_request":
			var voteRequest VoteRequest
			tmp, _ := json.Marshal(message.Data)
			json.Unmarshal(tmp, &voteRequest)
			voteResponse := n.handleVoteRequest(voteRequest)
			response = &Message{Key: "vote_response", Data: voteResponse}

		case "vote_response":
			var voteResponse VoteResponse
			tmp, _ := json.Marshal(message.Data)
			json.Unmarshal(tmp, &voteResponse)
			n.handleVoteResponse(voteResponse)

		case "heartbeat_request":
			n.handleHeartbeatRequest()

		default:

		}
		if response != nil {
			enc := json.NewEncoder(c)
			enc.Encode(response)
		}
	}
}

type VoteRequest struct {
	CandidateId   string `json:"candidateId"`
	CandidateTerm int    `json:"candidateTerm"`
}

func (n *Node) handleVoteRequest(request VoteRequest) *VoteResponse {
	if request.CandidateTerm > n.currentTerm {
		n.currentTerm = request.CandidateTerm
		n.currentRole = "follower"
		n.votedFor = ""
		n.votesCount = 0
		n.resetElectionTimer <- struct{}{}
	}

	if request.CandidateTerm == n.currentTerm /* && logOk */ && (n.votedFor == "" || n.votedFor == request.CandidateId) {
		n.votedFor = request.CandidateId
		return &VoteResponse{
			NodeId:      n.name,
			CurrentTerm: n.currentTerm,
			Accepted:    true,
		}
	} else {
		return &VoteResponse{
			NodeId:      n.name,
			CurrentTerm: n.currentTerm,
			Accepted:    true,
		}
	}
}

func (n *Node) handleVoteResponse(voteResponse VoteResponse) {
	if voteResponse.CurrentTerm > n.currentTerm {
		if n.currentRole != "leader" {
			n.resetElectionTimer <- struct{}{}
		}
		n.currentTerm = voteResponse.CurrentTerm
		n.currentRole = "follower"
		n.votedFor = ""
		n.votesCount = 0
	}
	if n.currentRole == "candidate" && voteResponse.CurrentTerm == n.currentTerm && voteResponse.Accepted {
		n.votesCount++
		n.checkForElectionResult()
	}
}

func (n *Node) handleHeartbeatRequest() {
	n.resetElectionTimer <- struct{}{}
}

func (n *Node) startElection() {
	fmt.Println("Start Election")
	n.cluster.leaderNode = nil

	n.currentTerm = n.currentTerm + 1
	n.currentRole = "candidate"
	n.votedFor = n.name
	n.votesCount = 1

	voteRequest := VoteRequest{
		CandidateId:   n.name,
		CandidateTerm: n.currentTerm,
	}
	for _, node := range n.cluster.nodes {
		if node.port != n.port {
			n.sendMessageToFollowerNode(Message{Key: "vote_request", Data: voteRequest}, node.port)
		}
	}
	n.checkForElectionResult()
}

func (n *Node) checkForElectionResult() {
	if n.currentRole == "leader" {
		return
	}

	if n.votesCount >= (len(n.cluster.nodes)+1)/2 {
		fmt.Println("New leader: ", n.name, " Votes received: ", n.votesCount)
		n.currentRole = "leader"
		n.leaderNodeId = n.name
		n.electionTimeout.Stop()
		n.cluster.leaderNode = n
		n.DoHeartbeat()
	}
}

func (n *Node) DoHeartbeat() {
	ticker := time.NewTicker(HeartBeat * time.Millisecond)
	for t := range ticker.C {
		time.AfterFunc(5*time.Second, func() {
			n.currentRole = "dead"
			ticker.Stop()
		})
		fmt.Println("sending heartbeat at: ", t)
		for _, node := range n.cluster.nodes {
			if node.port != n.port {
				n.sendMessageToFollowerNode(Message{"heartbeat_request", nil}, node.port)
			}
		}
	}

}

func (n *Node) electionTimer() {
	for {
		select {
		case <-n.electionTimeout.C:
			fmt.Println("Timed out ", n.name)
			if n.currentRole == "follower" {
				go n.startElection()
			} else {
				n.currentRole = "follower"
			}
		case <-n.resetElectionTimer:
			fmt.Println("Resetting election timer ", n.name)
			n.electionTimeout.Reset(time.Duration(n.electionTimeoutInterval) * time.Millisecond)
		}
	}
}

func (n *Node) sendMessageToFollowerNode(message Message, port string) {
	c, _ := net.Dial("tcp", "127.0.0.1:"+port)

	enc := json.NewEncoder(c)

	err := enc.Encode(message)
	if err != nil {
		log.Fatal(err.Error())
	}

	go n.handleConnection(c)
}

func (n *Node) kill() {
	n.currentRole = "dead"
}
