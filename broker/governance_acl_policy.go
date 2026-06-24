package broker

import (
	"fmt"
	"slices"
	"strings"

	"github.com/lyonbrown4d/ech0/store"
	"github.com/samber/lo"
)

func normalizeACLPolicy(identity Identity, policy ACLPolicy) ACLPolicy {
	identity = normalizeIdentity(identity)
	policy.PolicyID = strings.TrimSpace(policy.PolicyID)
	policy.Tenant = nonEmpty(strings.TrimSpace(policy.Tenant), identity.Tenant)
	policy.Namespace = nonEmpty(strings.TrimSpace(policy.Namespace), identity.Namespace)
	policy.Principal = normalizeACLWildcard(policy.Principal)
	policy.ResourceType = ACLResourceType(normalizeACLWildcard(string(policy.ResourceType)))
	policy.ResourceName = normalizeACLWildcard(policy.ResourceName)
	policy.Actions = normalizeACLActions(policy.Actions)
	if policy.Effect == "" {
		policy.Effect = ACLPolicyEffectAllow
	}
	if policy.UpdatedAtMS == 0 {
		policy.UpdatedAtMS = store.NowMS()
	}
	if policy.PolicyID == "" {
		policy.PolicyID = generatedACLPolicyID(policy)
	}
	return policy
}

func normalizeACLWildcard(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return aclPolicyWildcard
	}
	return value
}

func normalizeACLActions(actions []ACLAction) []ACLAction {
	if len(actions) == 0 {
		return []ACLAction{ACLAction(aclPolicyWildcard)}
	}
	return lo.Uniq(lo.Map(actions, func(action ACLAction, _ int) ACLAction {
		return ACLAction(normalizeACLWildcard(string(action)))
	}))
}

func aclActionsToStrings(actions []ACLAction) []string {
	if len(actions) == 0 {
		return nil
	}
	return lo.Map(actions, func(action ACLAction, _ int) string {
		return string(action)
	})
}

func stringsToACLActions(actions []string) []ACLAction {
	if len(actions) == 0 {
		return nil
	}
	return lo.Map(actions, func(action string, _ int) ACLAction {
		return ACLAction(action)
	})
}

func validateBrokerACLPolicy(policy ACLPolicy) error {
	if strings.TrimSpace(policy.PolicyID) == "" {
		return brokerStoreError(store.CodeInvalidArgument, "policy_id is required")
	}
	switch policy.Effect {
	case ACLPolicyEffectAllow, ACLPolicyEffectDeny:
	default:
		return brokerStoreError(store.CodeInvalidArgument, "acl policy %s has invalid effect %q", policy.PolicyID, policy.Effect)
	}
	if len(policy.Actions) == 0 {
		return brokerStoreError(store.CodeInvalidArgument, "acl policy %s must declare at least one action", policy.PolicyID)
	}
	return nil
}

func generatedACLPolicyID(policy ACLPolicy) string {
	actions := aclActionsToStrings(policy.Actions)
	slices.Sort(actions)
	return fmt.Sprintf(
		"%s/%s/%s/%s/%s/%s/%s",
		policy.Tenant,
		policy.Namespace,
		policy.Principal,
		policy.ResourceType,
		policy.ResourceName,
		strings.Join(actions, ","),
		policy.Effect,
	)
}

func brokerACLPolicyToStore(policy ACLPolicy) store.ACLPolicy {
	actions := aclActionsToStrings(policy.Actions)
	return store.ACLPolicy{
		PolicyID:     policy.PolicyID,
		Tenant:       policy.Tenant,
		Namespace:    policy.Namespace,
		Principal:    policy.Principal,
		ResourceType: string(policy.ResourceType),
		ResourceName: policy.ResourceName,
		Actions:      actions,
		Effect:       store.ACLPolicyEffect(policy.Effect),
		Priority:     policy.Priority,
		UpdatedAtMS:  policy.UpdatedAtMS,
	}
}

func storeACLPolicyToBroker(policy store.ACLPolicy) ACLPolicy {
	actions := stringsToACLActions(policy.Actions)
	return ACLPolicy{
		PolicyID:     policy.PolicyID,
		Tenant:       policy.Tenant,
		Namespace:    policy.Namespace,
		Principal:    policy.Principal,
		ResourceType: ACLResourceType(policy.ResourceType),
		ResourceName: policy.ResourceName,
		Actions:      actions,
		Effect:       ACLPolicyEffect(policy.Effect),
		Priority:     policy.Priority,
		UpdatedAtMS:  policy.UpdatedAtMS,
	}
}

func brokerACLPolicyFilterToStore(filter ACLPolicyFilter) store.ACLPolicyFilter {
	return store.ACLPolicyFilter{
		Tenant:       strings.TrimSpace(filter.Tenant),
		Namespace:    strings.TrimSpace(filter.Namespace),
		Principal:    strings.TrimSpace(filter.Principal),
		ResourceType: strings.TrimSpace(string(filter.ResourceType)),
		ResourceName: strings.TrimSpace(filter.ResourceName),
	}
}

func aclPolicyAdminResource(identity Identity) ACLResource {
	return ACLResource{
		Type:      ACLResourceAdmin,
		Tenant:    identity.Tenant,
		Namespace: identity.Namespace,
		Name:      "acl_policy",
	}
}
