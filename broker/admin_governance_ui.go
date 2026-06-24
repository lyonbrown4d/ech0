package broker

import (
	"strconv"

	"github.com/gofiber/fiber/v3"
	"github.com/samber/lo"
)

const governanceACLPreviewLimit = 5

type governanceView struct {
	Identity       Identity
	Quota          QuotaSummary
	QuotaError     string
	Auth           governanceAuthView
	TenantDefaults []governanceTenantDefaultView
	ACLPolicies    []ACLPolicy
	ACLPolicyCount int
	ACLError       string
}

type governanceAuthView struct {
	Enabled          bool
	AllowAnonymous   bool
	StaticTokenCount int
	StaticTokens     []governanceStaticTokenView
}

type governanceStaticTokenView struct {
	Principal string
	Tenant    string
	Namespace string
	ClientID  string
	Instance  string
}

type governanceTenantDefaultView struct {
	Tenant              string
	Namespace           string
	RetentionMaxBytes   uint64
	RetentionMS         string
	MessageTTLMS        string
	MessageExpiryAction string
	DelayEnabled        string
	RetryMaxAttempts    uint32
	DeadLetterTopic     string
}

func (s *AdminServer) uiGovernance(c fiber.Ctx) error {
	identity := identityFromContext(c.Context())
	view := governanceView{
		Identity:       identity,
		Auth:           governanceAuthFromConfig(s.effectiveGovernanceConfig().Auth),
		TenantDefaults: governanceTenantDefaultsFromConfig(s.effectiveGovernanceConfig().TenantDefaults),
	}
	quota, err := s.broker.QuotaSummaryFor(c.Context())
	if err != nil {
		view.QuotaError = err.Error()
	} else {
		view.Quota = quota
	}
	policies, err := s.broker.ListACLPolicies(c.Context(), ACLPolicyFilter{
		Tenant:    identity.Tenant,
		Namespace: identity.Namespace,
	})
	if err != nil {
		view.ACLError = err.Error()
	} else {
		view.ACLPolicyCount = len(policies)
		view.ACLPolicies = previewACLPolicies(policies)
	}
	return adminRender(c, "admin_templates/governance", view)
}

func (s *AdminServer) effectiveGovernanceConfig() GovernanceConfig {
	if s.broker != nil {
		return s.broker.cfg.Governance
	}
	return s.cfg.Governance
}

func governanceAuthFromConfig(cfg AuthConfig) governanceAuthView {
	return governanceAuthView{
		Enabled:          cfg.Enabled,
		AllowAnonymous:   cfg.AllowAnonymous,
		StaticTokenCount: len(cfg.StaticTokens),
		StaticTokens: lo.Map(cfg.StaticTokens, func(token StaticAuthTokenConfig, _ int) governanceStaticTokenView {
			return governanceStaticTokenView{
				Principal: token.Principal,
				Tenant:    token.Tenant,
				Namespace: token.Namespace,
				ClientID:  token.ClientID,
				Instance:  token.Instance,
			}
		}),
	}
}

func governanceTenantDefaultsFromConfig(defaults []TenantDefaultsConfig) []governanceTenantDefaultView {
	return lo.Map(defaults, func(item TenantDefaultsConfig, _ int) governanceTenantDefaultView {
		return governanceTenantDefaultView{
			Tenant:              item.Tenant,
			Namespace:           item.Namespace,
			RetentionMaxBytes:   item.RetentionMaxBytes,
			RetentionMS:         optionalUint64Value(item.RetentionMS),
			MessageTTLMS:        optionalUint64Value(item.MessageTTLMS),
			MessageExpiryAction: string(item.MessageExpiryAction),
			DelayEnabled:        optionalBoolValue(item.DelayEnabled),
			RetryMaxAttempts:    item.RetryPolicy.MaxAttempts,
			DeadLetterTopic:     item.DeadLetterTopic,
		}
	})
}

func previewACLPolicies(policies []ACLPolicy) []ACLPolicy {
	return lo.Take(policies, governanceACLPreviewLimit)
}

func optionalUint64Value(value *uint64) string {
	if value == nil {
		return "-"
	}
	return strconv.FormatUint(*value, 10)
}

func optionalBoolValue(value *bool) string {
	if value == nil {
		return "-"
	}
	return strconv.FormatBool(*value)
}
