package discovery

import (
	"context"

	"github.com/samber/lo"
)

type StaticProvider struct {
	local  Node
	nodes  []Node
	events chan Event
}

func NewStaticProvider(local Node, nodes []Node) *StaticProvider {
	return &StaticProvider{
		local:  local.Clone(),
		nodes:  cloneNodes(nodes),
		events: make(chan Event),
	}
}

func (p *StaticProvider) Start(context.Context) error {
	return nil
}

func (p *StaticProvider) Stop(context.Context) error {
	return nil
}

func (p *StaticProvider) LocalNode() Node {
	return p.local.Clone()
}

func (p *StaticProvider) Nodes() []Node {
	return cloneNodes(p.nodes)
}

func (p *StaticProvider) Events() <-chan Event {
	return p.events
}

func cloneNodes(nodes []Node) []Node {
	if len(nodes) == 0 {
		return nil
	}
	return lo.Map(nodes, func(node Node, _ int) Node {
		return node.Clone()
	})
}
