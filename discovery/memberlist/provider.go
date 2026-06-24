package memberlist

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	hashimemberlist "github.com/hashicorp/memberlist"
	"github.com/lyonbrown4d/ech0/discovery"
	"github.com/samber/lo"
)

const (
	defaultJoinTimeout = 10 * time.Second
	defaultJoinBackoff = 200 * time.Millisecond
)

type Config struct {
	LocalNode     discovery.Node
	BindAddr      string
	AdvertiseAddr string
	Seeds         []string
	JoinTimeout   time.Duration
	SecretKey     []byte
	Logger        *slog.Logger
}

type Provider struct {
	cfg    Config
	mu     sync.RWMutex
	list   *hashimemberlist.Memberlist
	events chan discovery.Event
}

func NewProvider(cfg Config) (*Provider, error) {
	if cfg.LocalNode.ID == 0 {
		return nil, errors.New("local discovery node id is required")
	}
	if cfg.LocalNode.ClusterName == "" {
		return nil, errors.New("local discovery cluster name is required")
	}
	if cfg.LocalNode.RaftAddr == "" {
		return nil, errors.New("local discovery raft address is required")
	}
	if cfg.BindAddr == "" {
		return nil, errors.New("memberlist bind address is required")
	}
	cfg.LocalNode.Status = discovery.NodeStatusAlive
	return &Provider{
		cfg:    cfg,
		events: make(chan discovery.Event, 64),
	}, nil
}

func (p *Provider) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.list != nil {
		p.mu.Unlock()
		return nil
	}
	mlcfg, err := p.memberlistConfig()
	if err != nil {
		p.mu.Unlock()
		return err
	}
	list, err := hashimemberlist.Create(mlcfg)
	if err != nil {
		p.mu.Unlock()
		return fmt.Errorf("create memberlist discovery provider: %w", err)
	}
	p.list = list
	p.mu.Unlock()

	if len(p.cfg.Seeds) == 0 {
		return nil
	}
	if err := p.joinSeeds(ctx); err != nil {
		return errors.Join(err, p.Stop(context.WithoutCancel(ctx)))
	}
	return nil
}

func (p *Provider) Stop(ctx context.Context) error {
	p.mu.Lock()
	list := p.list
	p.list = nil
	p.mu.Unlock()
	if list == nil {
		return nil
	}
	timeout := 2 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining < timeout {
			timeout = remaining
		}
	}
	if timeout < 0 {
		timeout = 0
	}
	return errors.Join(list.Leave(timeout), list.Shutdown())
}

func (p *Provider) LocalNode() discovery.Node {
	return p.cfg.LocalNode.Clone()
}

func (p *Provider) Nodes() []discovery.Node {
	p.mu.RLock()
	list := p.list
	p.mu.RUnlock()
	if list == nil {
		return []discovery.Node{p.LocalNode()}
	}
	members := list.Members()
	if len(members) == 0 {
		return nil
	}
	return lo.Map(members, func(member *hashimemberlist.Node, _ int) discovery.Node {
		return nodeFromMember(member)
	})
}

func (p *Provider) Events() <-chan discovery.Event {
	return p.events
}

func (p *Provider) memberlistConfig() (*hashimemberlist.Config, error) {
	host, port, err := splitHostPort(p.cfg.BindAddr)
	if err != nil {
		return nil, err
	}
	delegate := &delegate{provider: p, local: p.cfg.LocalNode}
	cfg := hashimemberlist.DefaultLANConfig()
	cfg.Name = nodeName(p.cfg.LocalNode)
	cfg.BindAddr = host
	cfg.BindPort = port
	cfg.Delegate = delegate
	cfg.Events = delegate
	cfg.Label = p.cfg.LocalNode.ClusterName
	cfg.SecretKey = p.cfg.SecretKey
	cfg.Logger = log.New(slogWriter{logger: p.cfg.Logger}, "", 0)
	if p.cfg.AdvertiseAddr != "" {
		advertiseHost, advertisePort, advertiseErr := splitAdvertiseHostPort(p.cfg.AdvertiseAddr)
		if advertiseErr != nil {
			return nil, advertiseErr
		}
		cfg.AdvertiseAddr = advertiseHost
		cfg.AdvertisePort = advertisePort
	}
	return cfg, nil
}

func (p *Provider) joinSeeds(ctx context.Context) error {
	timeout := p.cfg.JoinTimeout
	if timeout <= 0 {
		timeout = defaultJoinTimeout
	}
	joinCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	seeds := cloneStrings(p.cfg.Seeds)
	ticker := time.NewTicker(defaultJoinBackoff)
	defer ticker.Stop()
	var lastErr error
	for {
		p.mu.RLock()
		list := p.list
		p.mu.RUnlock()
		if list == nil {
			return errors.New("memberlist discovery provider is stopped")
		}
		joined, err := list.Join(seeds)
		if err == nil || joined > 0 {
			return nil
		}
		lastErr = err
		select {
		case <-joinCtx.Done():
			return fmt.Errorf("join memberlist seeds %v: %w", seeds, errors.Join(lastErr, joinCtx.Err()))
		case <-ticker.C:
		}
	}
}

func (p *Provider) emit(eventType discovery.EventType, node discovery.Node) {
	event := discovery.Event{Type: eventType, Node: node, At: time.Now()}
	select {
	case p.events <- event:
	default:
	}
}

func nodeName(node discovery.Node) string {
	if node.Name != "" {
		return node.Name
	}
	cluster := node.ClusterName
	if cluster == "" {
		cluster = "ech0"
	}
	return fmt.Sprintf("%s-%d", cluster, node.ID)
}

func splitHostPort(addr string) (string, int, error) {
	host, rawPort, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, fmt.Errorf("parse host port %q: %w", addr, err)
	}
	port, err := strconv.Atoi(rawPort)
	if err != nil || port <= 0 || port > 65535 {
		return "", 0, fmt.Errorf("parse port %q from %q: %w", rawPort, addr, err)
	}
	return host, port, nil
}

func splitAdvertiseHostPort(addr string) (string, int, error) {
	host, port, err := splitHostPort(addr)
	if err != nil {
		return "", 0, err
	}
	if net.ParseIP(host) == nil {
		return "", 0, fmt.Errorf("parse memberlist advertise address %q: host must be an IP address", addr)
	}
	return host, port, nil
}

func cloneStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := lo.FilterMap(values, func(value string, _ int) (string, bool) {
		return value, value != ""
	})
	if len(out) == 0 {
		return nil
	}
	return out
}
