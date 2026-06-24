package discovery

import (
	"context"
	"maps"
	"time"
)

type NodeStatus string

const (
	NodeStatusUnknown NodeStatus = ""
	NodeStatusAlive   NodeStatus = "alive"
	NodeStatusSuspect NodeStatus = "suspect"
	NodeStatusDead    NodeStatus = "dead"
	NodeStatusLeft    NodeStatus = "left"
)

type Node struct {
	ID             uint64            `json:"id"`
	Name           string            `json:"name,omitempty"`
	ClusterName    string            `json:"cluster_name,omitempty"`
	RaftAddr       string            `json:"raft_addr,omitempty"`
	BrokerAddr     string            `json:"broker_addr,omitempty"`
	AdminAddr      string            `json:"admin_addr,omitempty"`
	DataShardCount uint32            `json:"data_shard_count,omitempty"`
	Status         NodeStatus        `json:"status,omitempty"`
	Tags           map[string]string `json:"tags,omitempty"`
}

func (n Node) Alive() bool {
	return n.Status == NodeStatusUnknown || n.Status == NodeStatusAlive
}

func (n Node) ValidRaftPeer() bool {
	return n.ID != 0 && n.RaftAddr != ""
}

func (n Node) Clone() Node {
	n.Tags = cloneTags(n.Tags)
	return n
}

type EventType string

const (
	EventJoin   EventType = "join"
	EventLeave  EventType = "leave"
	EventUpdate EventType = "update"
)

type Event struct {
	Type EventType `json:"type"`
	Node Node      `json:"node"`
	At   time.Time `json:"at"`
}

type Provider interface {
	Start(context.Context) error
	Stop(context.Context) error
	LocalNode() Node
	Nodes() []Node
	Events() <-chan Event
}

func cloneTags(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	return maps.Clone(tags)
}
