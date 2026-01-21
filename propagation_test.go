package dbmq

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestTraceContextPropagation(t *testing.T) {
	// 创建一个 TracerProvider 用于生成真实的 span
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	tracer := tp.Tracer("test-tracer")

	// 创建一个带有 span 的 context
	ctx, span := tracer.Start(context.Background(), "test-span")
	defer span.End()

	originalSpanCtx := trace.SpanContextFromContext(ctx)
	t.Logf("Original TraceID: %s", originalSpanCtx.TraceID())
	t.Logf("Original SpanID: %s", originalSpanCtx.SpanID())

	// 使用 TraceContext propagator 注入到 headers
	headers := make(map[string]string)
	propagator := propagation.TraceContext{}
	propagator.Inject(ctx, propagation.MapCarrier(headers))

	t.Logf("Injected headers: %+v", headers)

	// 验证 headers 中有 traceparent
	traceparent, ok := headers["traceparent"]
	if !ok {
		t.Fatal("traceparent header not found")
	}
	t.Logf("traceparent: %s", traceparent)

	// 从 headers 中提取追踪上下文
	extractedCtx := propagator.Extract(context.Background(), propagation.MapCarrier(headers))
	extractedSpanCtx := trace.SpanContextFromContext(extractedCtx)

	t.Logf("Extracted TraceID: %s", extractedSpanCtx.TraceID())
	t.Logf("Extracted SpanID: %s", extractedSpanCtx.SpanID())

	// 验证提取的 trace-id 和 span-id 与原始一致
	if originalSpanCtx.TraceID() != extractedSpanCtx.TraceID() {
		t.Errorf("TraceID mismatch: got %s, want %s", extractedSpanCtx.TraceID(), originalSpanCtx.TraceID())
	}
	if originalSpanCtx.SpanID() != extractedSpanCtx.SpanID() {
		t.Errorf("SpanID mismatch: got %s, want %s", extractedSpanCtx.SpanID(), originalSpanCtx.SpanID())
	}

	// 验证提取的 context 是有效的
	if !extractedSpanCtx.IsValid() {
		t.Error("Extracted span context is not valid")
	}
}

func TestTraceContextPropagation_NoSpan(t *testing.T) {
	// 没有 span 的 context
	ctx := context.Background()

	headers := make(map[string]string)
	propagator := propagation.TraceContext{}
	propagator.Inject(ctx, propagation.MapCarrier(headers))

	t.Logf("Headers when no span: %+v", headers)

	// 没有 span 时，headers 应该为空
	if len(headers) != 0 {
		t.Errorf("Expected empty headers when no span, got: %+v", headers)
	}
}

func TestTraceContextPropagation_InvalidTraceparent(t *testing.T) {
	headers := map[string]string{
		"traceparent": "invalid-traceparent",
	}

	propagator := propagation.TraceContext{}
	extractedCtx := propagator.Extract(context.Background(), propagation.MapCarrier(headers))
	extractedSpanCtx := trace.SpanContextFromContext(extractedCtx)

	// 无效的 traceparent 应该返回无效的 span context
	if extractedSpanCtx.IsValid() {
		t.Error("Expected invalid span context for invalid traceparent")
	}
}
