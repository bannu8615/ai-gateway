// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package main

import (
	"context"
	"os"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/envoyproxy/ai-gateway/internal/controlplaneotel"
)

type harnessTelemetry struct {
	provider *controlplaneotel.Provider
	tracer   trace.Tracer
}

func newHarnessTelemetry(ctx context.Context) (*harnessTelemetry, error) {
	provider, err := controlplaneotel.New(ctx, os.Stdout, "ai-gateway-controlplane-perf", "envoyproxy/ai-gateway/controlplane-perf")
	if err != nil {
		return nil, err
	}
	return &harnessTelemetry{provider: provider, tracer: provider.Tracer()}, nil
}

func (h *harnessTelemetry) Shutdown(ctx context.Context) error {
	if h == nil || h.provider == nil {
		return nil
	}
	return h.provider.Shutdown(ctx)
}

func (h *harnessTelemetry) startRun(ctx context.Context, runID string) (context.Context, trace.Span) {
	return h.start(ctx, "controlplane.perf.run", attribute.String("controlplane.run_id", runID))
}

func (h *harnessTelemetry) startScenario(ctx context.Context, name string) (context.Context, trace.Span) {
	return h.start(ctx, "controlplane.perf.scenario", attribute.String("controlplane.scenario", name))
}

func (h *harnessTelemetry) startWave(ctx context.Context, scenario string, wave int, size int, cumulative int) (context.Context, trace.Span) {
	return h.start(ctx, "controlplane.perf.wave",
		attribute.String("controlplane.scenario", scenario),
		attribute.Int("controlplane.wave", wave),
		attribute.Int("controlplane.wave_size", size),
		attribute.Int("controlplane.cumulative_size", cumulative),
	)
}

func (h *harnessTelemetry) startObject(ctx context.Context, data templateData) (context.Context, trace.Span) {
	return h.start(ctx, "controlplane.perf.object",
		attribute.String("controlplane.run_id", data.RunID),
		attribute.String("controlplane.scenario", data.Scenario),
		attribute.Int("controlplane.wave", data.Wave),
		attribute.Int("controlplane.index", data.Index),
		attribute.String("k8s.object.name", data.Name),
	)
}

func (h *harnessTelemetry) startApply(ctx context.Context, obj *unstructured.Unstructured, mode string) (context.Context, trace.Span) {
	kind := ""
	namespace := ""
	name := ""
	if obj != nil {
		kind = obj.GetKind()
		namespace = obj.GetNamespace()
		name = obj.GetName()
	}
	return h.start(ctx, "controlplane.perf.apply",
		attribute.String("controlplane.operation", mode),
		attribute.String("k8s.kind", kind),
		attribute.String("k8s.namespace.name", namespace),
		attribute.String("k8s.object.name", name),
	)
}

func (h *harnessTelemetry) annotate(ctx context.Context, obj *unstructured.Unstructured, pushTime time.Time) {
	if h == nil || h.provider == nil || obj == nil {
		return
	}
	obj.SetAnnotations(controlplaneotel.Inject(ctx, h.provider.Propagator(), obj.GetAnnotations(), pushTime))
}

func (h *harnessTelemetry) addCreateError(result *instanceResult, err error) {
	if result == nil || result.traceSpan == nil || err == nil {
		return
	}
	recordSpanError(result.traceSpan, err)
}

func (h *harnessTelemetry) addHopSuccess(result *instanceResult, hopID string, observedAt time.Time, duration float64) {
	if result == nil || result.traceSpan == nil {
		return
	}
	result.traceSpan.AddEvent("hop.success", trace.WithAttributes(
		attribute.String("controlplane.hop", hopID),
		attribute.Float64("controlplane.duration_ms", duration),
		attribute.String("controlplane.observed_at", observedAt.UTC().Format(time.RFC3339Nano)),
	))
}

func (h *harnessTelemetry) addHopFailure(result *instanceResult, hopID string, message string) {
	if result == nil || result.traceSpan == nil {
		return
	}
	result.traceSpan.AddEvent("hop.failure", trace.WithAttributes(
		attribute.String("controlplane.hop", hopID),
		attribute.String("controlplane.error", message),
	))
}

func (h *harnessTelemetry) addWaveHopSuccess(span trace.Span, hopID string, observedAt time.Time, duration float64) {
	if span == nil {
		return
	}
	span.AddEvent("wave.hop.success", trace.WithAttributes(
		attribute.String("controlplane.hop", hopID),
		attribute.Float64("controlplane.duration_ms", duration),
		attribute.String("controlplane.observed_at", observedAt.UTC().Format(time.RFC3339Nano)),
	))
}

func (h *harnessTelemetry) addWaveHopFailure(span trace.Span, hopID string, message string) {
	if span == nil {
		return
	}
	span.AddEvent("wave.hop.failure", trace.WithAttributes(
		attribute.String("controlplane.hop", hopID),
		attribute.String("controlplane.error", message),
	))
}

func (h *harnessTelemetry) finishObject(result *instanceResult) {
	if result == nil || result.traceSpan == nil {
		return
	}
	if result.Namespace != "" {
		result.traceSpan.SetAttributes(attribute.String("k8s.namespace.name", result.Namespace))
	}
	if result.CreateError != "" {
		recordSpanError(result.traceSpan, errString(result.CreateError))
	} else {
		for _, hop := range result.Hops {
			if hop.Error != "" {
				recordSpanError(result.traceSpan, errString(hop.Error))
				break
			}
		}
	}
	result.traceSpan.End()
	result.traceSpan = nil
	result.traceCtx = nil
}

func finishInstanceSpans(telemetry *harnessTelemetry, instances []instanceResult) {
	for i := range instances {
		telemetry.finishObject(&instances[i])
	}
}

func (h *harnessTelemetry) start(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	if h == nil || h.tracer == nil {
		return ctx, noop.Span{}
	}
	return h.tracer.Start(ctx, name, trace.WithAttributes(attrs...))
}

type errString string

func (e errString) Error() string {
	return string(e)
}

func recordSpanError(span trace.Span, err error) {
	if span == nil || err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
