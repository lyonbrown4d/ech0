package protocol

import (
	"slices"
	"strings"

	collectionset "github.com/arcgolabs/collectionx/set"
)

const (
	CapabilityCompressionZstd   = "compression.zstd"
	CapabilityProduceBatch      = "produce.batch"
	CapabilityProduceBatches    = "produce.batches"
	CapabilityProduceFanout     = "produce.fanout"
	CapabilityFetchBatch        = "fetch.batch"
	CapabilityFetchWait         = "fetch.wait"
	CapabilityTransactions      = "transactions"
	CapabilityIdempotentProduce = "idempotent.produce"
	CapabilityDirect            = "direct"
	CapabilityRequestReply      = "request.reply"
	CapabilityRequestReplyMany  = "request.reply.many"
	CapabilityConsumerGroups    = "consumer.groups"
	CapabilityRetryDelay        = "retry.delay"
	CapabilityRoutingKey        = "routing.key"
	CapabilityTopicOrdering     = "topic.ordering"
	CapabilityTopicPriority     = "topic.priority"
	CapabilitySubjectWildcards  = "subject.wildcards"
	CapabilitySchemaHeaders     = "schema.headers"
)

var supportedCapabilities = []string{
	CapabilityCompressionZstd,
	CapabilityProduceBatch,
	CapabilityProduceBatches,
	CapabilityProduceFanout,
	CapabilityFetchBatch,
	CapabilityFetchWait,
	CapabilityTransactions,
	CapabilityIdempotentProduce,
	CapabilityDirect,
	CapabilityRequestReply,
	CapabilityRequestReplyMany,
	CapabilityConsumerGroups,
	CapabilityRetryDelay,
	CapabilityRoutingKey,
	CapabilityTopicOrdering,
	CapabilityTopicPriority,
	CapabilitySubjectWildcards,
	CapabilitySchemaHeaders,
}

func SupportedCapabilities() []string {
	if len(supportedCapabilities) == 0 {
		return nil
	}
	return slices.Clone(supportedCapabilities)
}

func NegotiateCapabilities(requested []string) []string {
	if len(requested) == 0 {
		return SupportedCapabilities()
	}
	want := collectionset.NewSet[string]()
	for _, capability := range requested {
		capability = strings.TrimSpace(capability)
		if capability != "" {
			want.Add(capability)
		}
	}
	var out []string
	for _, capability := range supportedCapabilities {
		if want.Contains(capability) {
			out = append(out, capability)
		}
	}
	return out
}
