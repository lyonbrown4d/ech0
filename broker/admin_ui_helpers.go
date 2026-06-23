package broker

import (
	"net/url"
	"strconv"
	"strings"

	collectionlist "github.com/arcgolabs/collectionx/list"
	"github.com/gofiber/fiber/v3"
)

func parseUint32Query(c fiber.Ctx, key string) uint32 {
	value, err := strconv.ParseUint(c.Query(key, "0"), 10, 32)
	if err != nil {
		return 0
	}
	return uint32(value)
}

func parseUint64Query(c fiber.Ctx, key string) uint64 {
	value, err := strconv.ParseUint(c.Query(key, "0"), 10, 64)
	if err != nil {
		return 0
	}
	return value
}

func parseIntQuery(c fiber.Ctx, key string, fallback int) int {
	value, err := strconv.Atoi(strings.TrimSpace(c.Query(key)))
	if err != nil {
		return fallback
	}
	return value
}

func parseIntForm(c fiber.Ctx, key string) int {
	value, err := strconv.Atoi(strings.TrimSpace(c.FormValue(key)))
	if err != nil {
		return 0
	}
	return value
}

func aclPolicyFilterFromQuery(c fiber.Ctx) ACLPolicyFilter {
	return ACLPolicyFilter{
		Tenant:       strings.TrimSpace(c.Query("tenant")),
		Namespace:    strings.TrimSpace(c.Query("namespace")),
		Principal:    strings.TrimSpace(c.Query("principal")),
		ResourceType: ACLResourceType(strings.TrimSpace(c.Query("resource_type"))),
		ResourceName: strings.TrimSpace(c.Query("resource_name")),
	}
}

func aclPolicyFromForm(c fiber.Ctx) ACLPolicy {
	return ACLPolicy{
		PolicyID:     strings.TrimSpace(c.FormValue("policy_id")),
		Tenant:       strings.TrimSpace(c.FormValue("tenant")),
		Namespace:    strings.TrimSpace(c.FormValue("namespace")),
		Principal:    strings.TrimSpace(c.FormValue("principal")),
		ResourceType: ACLResourceType(strings.TrimSpace(c.FormValue("resource_type"))),
		ResourceName: strings.TrimSpace(c.FormValue("resource_name")),
		Actions:      parseACLActions(c.FormValue("actions")),
		Effect:       ACLPolicyEffect(strings.TrimSpace(c.FormValue("effect"))),
		Priority:     parseIntForm(c, "priority"),
	}
}

func parseACLActions(value string) []ACLAction {
	parts := strings.Split(value, ",")
	actions := collectionlist.NewList[ACLAction]()
	for _, part := range parts {
		action := strings.TrimSpace(part)
		if action != "" {
			actions.Add(ACLAction(action))
		}
	}
	return actions.Values()
}

func aclActionList(actions []ACLAction) string {
	values := collectionlist.NewListWithCapacity[string](len(actions))
	for _, action := range actions {
		values.Add(string(action))
	}
	return strings.Join(values.Values(), ",")
}

func redirectACLPolicies(c fiber.Ctx, tenant, namespace string) error {
	target := "/ui/acls"
	query := url.Values{}
	if tenant != "" {
		query.Set("tenant", tenant)
	}
	if namespace != "" {
		query.Set("namespace", namespace)
	}
	if encoded := query.Encode(); encoded != "" {
		target += "?" + encoded
	}
	return wrapBroker("admin_acl_redirect_failed", c.Redirect().Status(fiber.StatusSeeOther).To(target), "redirect acl policies")
}

func redirectACLPolicyError(c fiber.Ctx, err error) error {
	target := "/ui/acls?error=" + url.QueryEscape(err.Error())
	return wrapBroker("admin_acl_error_redirect_failed", c.Redirect().Status(fiber.StatusSeeOther).To(target), "redirect acl policy error")
}
