package dbmq

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// TestProducerConsumerSpanCorrelation 验证 producer span 与 consumer span 的关联。
//
// 复现真实链路：producer 创建 send span → Inject 到 Headers → consumer 调用
// StartConsumerSpan 从 Headers 提取并创建 process span，然后断言两者的 trace 关系。
//
// 期望（修复后）：
//  1. consumer span 与 producer span 在同一条 trace（相同 TraceID）—— SigNoz 可见串联。
//  2. consumer span 的 parent 指向 producer span。
//  3. consumer span 同时保留一条 Link 指向 producer（满足 OTEL MQ 语义）。
func TestProducerConsumerSpanCorrelation(t *testing.T) {
	tp := sdktrace.NewTracerProvider()
	defer func() { _ = tp.Shutdown(context.Background()) }()

	// 让 StartConsumerSpan 内部的 otel.Tracer(...) 走真实 SDK
	prev := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	defer otel.SetTracerProvider(prev)

	// ---- Producer 侧：复刻 producer.go 的注入逻辑 ----
	producerTracer := tp.Tracer("dbmq.producer")
	producerCtx, producerSpan := producerTracer.Start(context.Background(), "send",
		trace.WithSpanKind(trace.SpanKindProducer))
	producerSC := trace.SpanContextFromContext(producerCtx)
	t.Logf("Producer  TraceID=%s SpanID=%s", producerSC.TraceID(), producerSC.SpanID())

	headers := make(map[string]string)
	propagation.TraceContext{}.Inject(producerCtx, propagation.MapCarrier(headers))
	producerSpan.End()

	// ---- Consumer 侧：调用真实的 StartConsumerSpan ----
	// consumer 拿到的是一个与 producer 完全无关的全新请求 context
	msg := ConsumerMessage{Topic: "demo-topic", Partition: 0, ID: 1, Headers: headers}
	consumerCtx, consumerSpan := msg.StartConsumerSpan(context.Background(), "process")
	consumerSC := trace.SpanContextFromContext(consumerCtx)
	t.Logf("Consumer  TraceID=%s SpanID=%s", consumerSC.TraceID(), consumerSC.SpanID())

	roSpan := consumerSpan.(sdktrace.ReadOnlySpan)
	parent := roSpan.Parent()
	gotLinks := roSpan.Links()
	consumerSpan.End()

	t.Logf("Consumer parent: TraceID=%s SpanID=%s valid=%v",
		parent.TraceID(), parent.SpanID(), parent.IsValid())
	t.Logf("Consumer links count=%d", len(gotLinks))

	// 1. 同一条 trace
	if consumerSC.TraceID() != producerSC.TraceID() {
		t.Errorf("consumer 与 producer 不在同一条 trace 上 (consumer=%s, producer=%s)",
			consumerSC.TraceID(), producerSC.TraceID())
	}

	// 2. parent 指向 producer span
	if parent.SpanID() != producerSC.SpanID() {
		t.Errorf("consumer span 的 parent 不是 producer span (parent=%s, producer=%s)",
			parent.SpanID(), producerSC.SpanID())
	}

	// 3. 保留一条指向 producer 的 Link
	if len(gotLinks) != 1 {
		t.Fatalf("期望 1 条 Span Link，实际 %d 条", len(gotLinks))
	}
	if gotLinks[0].SpanContext.SpanID() != producerSC.SpanID() {
		t.Errorf("Link 未指向 producer span (link=%s, producer=%s)",
			gotLinks[0].SpanContext.SpanID(), producerSC.SpanID())
	}
}
