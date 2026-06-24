package client

import (
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lyonbrown4d/ech0/protocol"
)

const (
	defaultDialTimeout      = 5 * time.Second
	defaultOperationTimeout = 30 * time.Second
	defaultPoolSize         = 8
	defaultMaxFrameBody     = 8 * 1024 * 1024
)

var nextClientID atomic.Uint64

type Option func(*Options)

type Options struct {
	ClientID          string
	Tenant            string
	Namespace         string
	Principal         string
	AuthToken         string
	Capabilities      []string
	DialTimeout       time.Duration
	OperationTimeout  time.Duration
	PoolSize          int
	MaxFrameBodyBytes uint32
}

func WithClientID(clientID string) Option {
	return func(opts *Options) {
		opts.ClientID = strings.TrimSpace(clientID)
	}
}

func WithTenant(tenant string) Option {
	return func(opts *Options) {
		opts.Tenant = strings.TrimSpace(tenant)
	}
}

func WithNamespace(namespace string) Option {
	return func(opts *Options) {
		opts.Namespace = strings.TrimSpace(namespace)
	}
}

func WithPrincipal(principal string) Option {
	return func(opts *Options) {
		opts.Principal = strings.TrimSpace(principal)
	}
}

func WithAuthToken(token string) Option {
	return func(opts *Options) {
		opts.AuthToken = strings.TrimSpace(token)
	}
}

func WithCapabilities(capabilities ...string) Option {
	return func(opts *Options) {
		opts.Capabilities = slices.Clone(capabilities)
	}
}

func WithDialTimeout(timeout time.Duration) Option {
	return func(opts *Options) {
		if timeout > 0 {
			opts.DialTimeout = timeout
		}
	}
}

func WithOperationTimeout(timeout time.Duration) Option {
	return func(opts *Options) {
		if timeout > 0 {
			opts.OperationTimeout = timeout
		}
	}
}

func WithPoolSize(size int) Option {
	return func(opts *Options) {
		if size > 0 {
			opts.PoolSize = size
		}
	}
}

func WithMaxFrameBodyBytes(limit uint32) Option {
	return func(opts *Options) {
		if limit > 0 {
			opts.MaxFrameBodyBytes = limit
		}
	}
}

func normalizeOptions(opts []Option) Options {
	out := Options{
		ClientID:          defaultClientID(),
		Capabilities:      protocol.SupportedCapabilities(),
		DialTimeout:       defaultDialTimeout,
		OperationTimeout:  defaultOperationTimeout,
		PoolSize:          defaultPoolSize,
		MaxFrameBodyBytes: defaultMaxFrameBody,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&out)
		}
	}
	if out.ClientID == "" {
		out.ClientID = defaultClientID()
	}
	out.Capabilities = slices.Clone(out.Capabilities)
	return out
}

func defaultClientID() string {
	next := nextClientID.Add(1)
	return "ech0-go-" + strconv.FormatInt(time.Now().UnixNano(), 10) + "-" + strconv.FormatUint(next, 10)
}
