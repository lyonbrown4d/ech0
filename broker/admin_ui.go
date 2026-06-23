package broker

import (
	"net/http"
	"strconv"

	"github.com/gofiber/fiber/v3"
	fiberhtml "github.com/gofiber/template/html/v3"
)

func adminTemplateEngine() *fiberhtml.Engine {
	engine := fiberhtml.NewFileSystem(http.FS(adminTemplates), ".html")
	engine.AddFunc("fmtOffset", displayUint64Ptr)
	engine.AddFunc("fmtUint", strconv.FormatUint)
	engine.AddFunc("aclActions", aclActionList)
	engine.AddFunc("dict", adminTemplateDict)
	return engine
}

func (s *AdminServer) uiDashboard(c fiber.Ctx) error {
	if err := s.metrics.RefreshStream(c.Context(), s.broker); err != nil && s.logger != nil {
		s.logger.Warn("refresh metrics failed", "error", err)
	}
	topics, err := s.broker.TopicSummariesFor(c.Context())
	view := dashboardView{
		Health:  s.broker.RuntimeHealth(),
		Metrics: s.metrics.Snapshot(),
	}
	if err != nil {
		view.TopicsError = err.Error()
	} else {
		view.Topics = topics
	}
	quota, quotaErr := s.broker.QuotaSummaryFor(c.Context())
	if quotaErr != nil {
		view.QuotaError = quotaErr.Error()
	} else {
		view.Quota = quota
	}
	view.CommandErrorRate = commandErrorRate(view.Metrics)
	return adminRender(c, "admin_templates/dashboard", view)
}

func commandErrorRate(metrics MetricsSnapshot) string {
	if metrics.CommandsTotal == 0 {
		return "0.00"
	}
	rate := float64(metrics.CommandErrorsTotal) / float64(metrics.CommandsTotal) * 100
	return strconv.FormatFloat(rate, 'f', 2, 64)
}

func (s *AdminServer) uiTopics(c fiber.Ctx) error {
	topics, err := s.broker.TopicSummariesFor(c.Context())
	view := topicsView{Topics: topics}
	if err != nil {
		view.Error = err.Error()
	}
	return adminRender(c, "admin_templates/topics", view)
}

func (s *AdminServer) uiACLPolicies(c fiber.Ctx) error {
	filter := aclPolicyFilterFromQuery(c)
	policies, err := s.broker.ListACLPolicies(c.Context(), filter)
	view := aclPoliciesView{
		Policies:  policies,
		Error:     c.Query("error"),
		Tenant:    filter.Tenant,
		Namespace: filter.Namespace,
		Principal: filter.Principal,
	}
	if err != nil {
		view.Error = err.Error()
	}
	return adminRender(c, "admin_templates/acl_policies", view)
}

func (s *AdminServer) uiTopicMessages(c fiber.Ctx) error {
	topic := c.Params("topic")
	partition := parseUint32Query(c, "partition")
	offset := parseUint64Query(c, "offset")
	limit := parseIntQuery(c, "limit", 50)
	cursor := c.Query("cursor")
	page, err := s.broker.TopicMessagesSnapshotFor(c.Context(), topic, partition, offset, limit)
	if cursor != "" || offset == 0 {
		page, err = s.broker.TopicMessagesCursorSnapshotFor(c.Context(), topic, partition, cursor, limit)
	}
	view := topicMessagesView{
		Page:       page,
		PrevOffset: previousOffset(offset, limit),
	}
	if err != nil {
		view.Error = err.Error()
		view.Page = TopicMessagesPageSummary{Topic: topic, Partition: partition, Offset: offset, Limit: limit, Cursor: cursor, NextOffset: offset}
	}
	return adminRender(c, "admin_templates/topic_messages", view)
}

func previousOffset(offset uint64, limit int) uint64 {
	if limit > 0 && offset > uint64(limit) {
		return offset - uint64(limit)
	}
	return 0
}

func (s *AdminServer) uiGroup(c fiber.Ctx) error {
	group := c.Params("group")
	health, err := s.broker.GroupHealthSnapshotFor(c.Context(), group)
	view := groupView{Group: group, Health: health}
	if health != nil {
		view.Explain = health.RebalanceExplain
		view.Members = health.Members
		view.Assignment = health.Assignment
		view.Lag = health.Lag
	}
	if err != nil {
		view.Error = err.Error()
	}
	return adminRender(c, "admin_templates/group", view)
}

func (s *AdminServer) apiGroupHealth(c fiber.Ctx) error {
	health, err := s.broker.GroupHealthSnapshotFor(c.Context(), c.Params("group"))
	if err != nil {
		return adminJSONError(c, err)
	}
	return adminJSON(c, health)
}

func (s *AdminServer) apiGroupMembers(c fiber.Ctx) error {
	members, err := s.broker.GroupMembersSnapshotFor(c.Context(), c.Params("group"))
	if err != nil {
		return adminJSONError(c, err)
	}
	return adminJSON(c, members)
}

func (s *AdminServer) apiGroupAssignment(c fiber.Ctx) error {
	assignment, err := s.broker.GroupAssignmentSnapshotFor(c.Context(), c.Params("group"))
	if err != nil {
		return adminJSONError(c, err)
	}
	return adminJSON(c, assignment)
}

func (s *AdminServer) apiGroupLag(c fiber.Ctx) error {
	lag, err := s.broker.GroupLagSnapshotFor(c.Context(), c.Params("group"))
	if err != nil {
		return adminJSONError(c, err)
	}
	return adminJSON(c, lag)
}

func (s *AdminServer) apiGroupRebalance(c fiber.Ctx) error {
	assignment, err := s.broker.RebalanceConsumerGroup(c.Context(), c.Params("group"))
	if err != nil {
		return adminJSONError(c, err)
	}
	return adminJSON(c, groupAssignmentSummary(assignment))
}

func (s *AdminServer) apiGroupRebalanceExplain(c fiber.Ctx) error {
	explain, err := s.broker.GroupRebalanceExplainFor(c.Context(), c.Params("group"))
	if err != nil {
		return adminJSONError(c, err)
	}
	return adminJSON(c, explain)
}

func (s *AdminServer) apiACLPolicies(c fiber.Ctx) error {
	policies, err := s.broker.ListACLPolicies(c.Context(), aclPolicyFilterFromQuery(c))
	if err != nil {
		return adminJSONError(c, err)
	}
	return adminJSON(c, policies)
}

func (s *AdminServer) apiACLPolicyUpsert(c fiber.Ctx) error {
	policy := aclPolicyFromForm(c)
	created, err := s.broker.UpsertACLPolicy(c.Context(), policy)
	if err != nil {
		return redirectACLPolicyError(c, err)
	}
	return redirectACLPolicies(c, created.Tenant, created.Namespace)
}

func (s *AdminServer) apiACLPolicyDelete(c fiber.Ctx) error {
	if err := s.broker.DeleteACLPolicy(c.Context(), c.FormValue("policy_id")); err != nil {
		return redirectACLPolicyError(c, err)
	}
	return redirectACLPolicies(c, c.FormValue("tenant"), c.FormValue("namespace"))
}

func adminJSONError(c fiber.Ctx, err error) error {
	response := adminErrorResponse{Error: err.Error()}
	return wrapBroker("admin_json_error_failed", c.Status(fiber.StatusInternalServerError).JSON(response), "write admin json error")
}

func adminRender(c fiber.Ctx, name string, view any) error {
	return wrapBroker("admin_render_failed", c.Render(name, view), "render admin template %s", name)
}

func adminJSON(c fiber.Ctx, value any) error {
	return wrapBroker("admin_json_failed", c.JSON(value), "write admin json")
}

type adminErrorResponse struct {
	Error string `json:"error"`
}
