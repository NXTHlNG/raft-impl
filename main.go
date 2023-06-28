package main

import (
	"fmt"
	"net"
)

type Cluster struct {
	nodes map[string]Node
	//ports      *[]string
	leaderNode *Node
}

func NewCluster() *Cluster {
	return &Cluster{
		nodes: map[string]Node{},
	}
}

func (c *Cluster) AddNode(name string, port string) {
	println(name, port)

	//*c.ports = append(*c.ports, port)

	node := NewNode(name, port, c)

	c.nodes[name] = *node

	go node.electionTimer()

	ln, _ := net.Listen("tcp", ":"+node.port)

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println(err)
		}
		go node.handleConnection(conn)
	}
}

func main() {
	cluster := NewCluster()

	go cluster.AddNode("A", "8001")
	go cluster.AddNode("B", "8002")
	go cluster.AddNode("C", "8003")
	go cluster.AddNode("D", "8004")
	go cluster.AddNode("E", "8005")

	//time.AfterFunc(15*time.Second, func() {
	//	fmt.Println(cluster.leaderNode)
	//	cluster.RemoveNode(cluster.leaderNode.name)
	//})

	for {

	}
}
