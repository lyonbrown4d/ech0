package broker

import (
	"net/url"
	"strconv"
	"strings"

	"github.com/gofiber/fiber/v3"
	"github.com/samber/lo"
)

type operationsView struct {
	Cluster          ClusterMetadata
	Result           string
	Error            string
	WebhookSinks     []operationSinkView
	FileSinks        []operationSinkView
	MirrorSinks      []operationSinkView
	S3Sinks          []operationSinkView
	DatabaseOutboxes []operationSinkView
}

type operationSinkView struct {
	Name      string
	Topic     string
	Partition uint32
	Target    string
	Action    string
}

func (s *AdminServer) uiOperations(c fiber.Ctx) error {
	return adminRender(c, "admin_templates/ops", operationsView{
		Cluster:          s.broker.ClusterMetadata(),
		Result:           c.Query("result"),
		Error:            c.Query("error"),
		WebhookSinks:     webhookSinkViews(s.cfg.Broker.WebhookSinks),
		FileSinks:        fileSinkViews(s.cfg.Broker.FileSinks),
		MirrorSinks:      mirrorSinkViews(s.cfg.Broker.MirrorSinks),
		S3Sinks:          s3SinkViews(s.cfg.Broker.S3Sinks),
		DatabaseOutboxes: databaseOutboxViews(s.cfg.Broker.DatabaseOutboxes),
	})
}

func (s *AdminServer) apiRunWebhookSink(c fiber.Ctx) error {
	sink, ok := findWebhookSink(s.cfg.Broker.WebhookSinks, c.FormValue("name"))
	if !ok {
		return redirectOpsError(c, "webhook sink not found")
	}
	result, err := s.broker.ProcessWebhookSinkOnce(c.Context(), sink)
	return redirectOpsResult(c, "webhook delivered "+strconv.Itoa(result.Delivered), err)
}

func (s *AdminServer) apiRunFileSink(c fiber.Ctx) error {
	sink, ok := findFileSink(s.cfg.Broker.FileSinks, c.FormValue("name"))
	if !ok {
		return redirectOpsError(c, "file sink not found")
	}
	result, err := s.broker.ProcessFileSinkOnce(c.Context(), sink)
	return redirectOpsResult(c, "file wrote "+strconv.Itoa(result.Delivered), err)
}

func (s *AdminServer) apiRunMirrorSink(c fiber.Ctx) error {
	sink, ok := findMirrorSink(s.cfg.Broker.MirrorSinks, c.FormValue("name"))
	if !ok {
		return redirectOpsError(c, "mirror sink not found")
	}
	result, err := s.broker.ProcessMirrorSinkOnce(c.Context(), sink)
	return redirectOpsResult(c, "mirror replicated "+strconv.Itoa(result.Delivered), err)
}

func (s *AdminServer) apiRunS3Sink(c fiber.Ctx) error {
	sink, ok := findS3Sink(s.cfg.Broker.S3Sinks, c.FormValue("name"))
	if !ok {
		return redirectOpsError(c, "s3 sink not found")
	}
	result, err := s.broker.ProcessS3SinkOnce(c.Context(), sink)
	return redirectOpsResult(c, "s3 wrote "+strconv.Itoa(result.Delivered), err)
}

func (s *AdminServer) apiRunDatabaseOutbox(c fiber.Ctx) error {
	outbox, ok := findDatabaseOutbox(s.cfg.Broker.DatabaseOutboxes, c.FormValue("name"))
	if !ok {
		return redirectOpsError(c, "database outbox not found")
	}
	result, err := s.broker.ProcessDatabaseOutboxOnce(c.Context(), outbox)
	return redirectOpsResult(c, "outbox published "+strconv.Itoa(result.Published), err)
}

func webhookSinkViews(sinks []WebhookSinkConfig) []operationSinkView {
	return operationSinkViews(sinks, func(sink WebhookSinkConfig) operationSinkView {
		return operationSinkView{Name: webhookSinkName(sink), Topic: sink.Topic, Partition: sink.Partition, Target: sink.URL, Action: "/api/ops/webhook-sinks/run"}
	})
}

func fileSinkViews(sinks []FileSinkConfig) []operationSinkView {
	return operationSinkViews(sinks, func(sink FileSinkConfig) operationSinkView {
		return operationSinkView{Name: fileSinkName(sink), Topic: sink.Topic, Partition: sink.Partition, Target: sink.Path, Action: "/api/ops/file-sinks/run"}
	})
}

func mirrorSinkViews(sinks []MirrorSinkConfig) []operationSinkView {
	return operationSinkViews(sinks, func(sink MirrorSinkConfig) operationSinkView {
		return operationSinkView{Name: mirrorSinkName(sink), Topic: sink.Topic, Partition: sink.Partition, Target: mirrorSinkAdminURL(sink), Action: "/api/ops/mirror-sinks/run"}
	})
}

func s3SinkViews(sinks []S3SinkConfig) []operationSinkView {
	return operationSinkViews(sinks, func(sink S3SinkConfig) operationSinkView {
		return operationSinkView{Name: s3SinkName(sink), Topic: sink.Topic, Partition: sink.Partition, Target: strings.TrimSpace(sink.Bucket), Action: "/api/ops/s3-sinks/run"}
	})
}

func databaseOutboxViews(outboxes []DatabaseOutboxConfig) []operationSinkView {
	return operationSinkViews(outboxes, func(outbox DatabaseOutboxConfig) operationSinkView {
		return operationSinkView{Name: databaseOutboxName(outbox), Topic: outbox.Topic, Target: strings.TrimSpace(outbox.DriverName), Action: "/api/ops/database-outboxes/run"}
	})
}

func operationSinkViews[T any](items []T, mapper func(T) operationSinkView) []operationSinkView {
	return lo.Map(items, func(item T, _ int) operationSinkView {
		return mapper(item)
	})
}

func findWebhookSink(sinks []WebhookSinkConfig, name string) (WebhookSinkConfig, bool) {
	return findOperationConfig(sinks, name, webhookSinkName)
}

func findFileSink(sinks []FileSinkConfig, name string) (FileSinkConfig, bool) {
	return findOperationConfig(sinks, name, fileSinkName)
}

func findMirrorSink(sinks []MirrorSinkConfig, name string) (MirrorSinkConfig, bool) {
	return findOperationConfig(sinks, name, mirrorSinkName)
}

func findS3Sink(sinks []S3SinkConfig, name string) (S3SinkConfig, bool) {
	return findOperationConfig(sinks, name, s3SinkName)
}

func findDatabaseOutbox(outboxes []DatabaseOutboxConfig, name string) (DatabaseOutboxConfig, bool) {
	return findOperationConfig(outboxes, name, databaseOutboxName)
}

func findOperationConfig[T any](items []T, name string, nameOf func(T) string) (T, bool) {
	target := strings.TrimSpace(name)
	return lo.Find(items, func(item T) bool {
		return nameOf(item) == target
	})
}

func redirectOpsResult(c fiber.Ctx, result string, err error) error {
	if err != nil {
		return redirectOpsError(c, err.Error())
	}
	return redirectOps(c, "result", result)
}

func redirectOpsError(c fiber.Ctx, message string) error {
	return redirectOps(c, "error", message)
}

func redirectOps(c fiber.Ctx, key, value string) error {
	target := "/ui/ops"
	if strings.TrimSpace(value) != "" {
		target += "?" + key + "=" + url.QueryEscape(value)
	}
	return wrapBroker("admin_ops_redirect_failed", c.Redirect().Status(fiber.StatusSeeOther).To(target), "redirect operations")
}
