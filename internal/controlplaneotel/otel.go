// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package controlplaneotel

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/exporters/autoexport"
	"go.opentelemetry.io/contrib/propagators/autoprop"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type Provider struct {
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
	shutdown   func(context.Context) error
}

func New(ctx context.Context, stdout io.Writer, serviceName string, tracerName string) (*Provider, error) {
	propagator := autoprop.NewTextMapPropagator()
	if os.Getenv("OTEL_SDK_DISABLED") == "true" {
		return &Provider{tracer: noop.NewTracerProvider().Tracer(tracerName), propagator: propagator}, nil
	}

	exporter := os.Getenv("OTEL_TRACES_EXPORTER")
	if exporter == "none" {
		return &Provider{tracer: noop.NewTracerProvider().Tracer(tracerName), propagator: propagator}, nil
	}
	if exporter == "" {
		hasOTLPEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" ||
			os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != ""
		if !hasOTLPEndpoint {
			return &Provider{tracer: noop.NewTracerProvider().Tracer(tracerName), propagator: propagator}, nil
		}
	}

	defaultRes := resource.Default()
	envRes, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
	)
	if err != nil {
		return nil, fmt.Errorf("create resource from env: %w", err)
	}
	fallbackRes := resource.NewSchemaless(attribute.String("service.name", serviceName))
	res, err := resource.Merge(defaultRes, fallbackRes)
	if err != nil {
		return nil, fmt.Errorf("merge default resources: %w", err)
	}
	res, err = resource.Merge(res, envRes)
	if err != nil {
		return nil, fmt.Errorf("merge env resources: %w", err)
	}

	var tp *sdktrace.TracerProvider
	if exporter == "console" {
		stdoutExporter, err := stdouttrace.New(stdouttrace.WithWriter(stdout))
		if err != nil {
			return nil, fmt.Errorf("create console exporter: %w", err)
		}
		tp = sdktrace.NewTracerProvider(
			sdktrace.WithSyncer(stdoutExporter),
			sdktrace.WithResource(res),
		)
	} else {
		autoExporter, err := autoexport.NewSpanExporter(ctx)
		if err != nil {
			return nil, fmt.Errorf("create exporter: %w", err)
		}
		tp = sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(autoExporter),
			sdktrace.WithResource(res),
		)
	}

	return &Provider{
		tracer:     tp.Tracer(tracerName),
		propagator: propagator,
		shutdown:   tp.Shutdown,
	}, nil
}

func (p *Provider) Tracer() trace.Tracer {
	if p == nil {
		return noop.NewTracerProvider().Tracer("")
	}
	return p.tracer
}

func (p *Provider) Propagator() propagation.TextMapPropagator {
	if p == nil {
		return propagation.NewCompositeTextMapPropagator()
	}
	return p.propagator
}

func (p *Provider) Shutdown(ctx context.Context) error {
	if p == nil || p.shutdown == nil {
		return nil
	}
	return p.shutdown(ctx)
}

const (
	pushTimestampAnnotation = "aigateway.envoyproxy.io/controlplane-push-timestamp"
	annotationPrefix        = "aigateway.envoyproxy.io/otel-"
)

type annotationCarrier map[string]string

func (c annotationCarrier) Get(key string) string {
	return c[annotationKey(key)]
}

func (c annotationCarrier) Set(key string, value string) {
	c[annotationKey(key)] = value
}

func (c annotationCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for key := range c {
		if strings.HasPrefix(key, annotationPrefix) {
			keys = append(keys, strings.TrimPrefix(key, annotationPrefix))
		}
	}
	return keys
}

func annotationKey(key string) string {
	return annotationPrefix + strings.ToLower(key)
}

func Inject(ctx context.Context, propagator propagation.TextMapPropagator, annotations map[string]string, pushTime time.Time) map[string]string {
	result := make(map[string]string, len(annotations)+4)
	for key, value := range annotations {
		result[key] = value
	}
	if propagator != nil {
		propagator.Inject(ctx, annotationCarrier(result))
	}
	if !pushTime.IsZero() {
		result[pushTimestampAnnotation] = pushTime.UTC().Format(time.RFC3339Nano)
	}
	return result
}

func Extract(propagator propagation.TextMapPropagator, annotations map[string]string) context.Context {
	if propagator == nil {
		return context.Background()
	}
	return propagator.Extract(context.Background(), annotationCarrier(annotations))
}

func PushTimestamp(annotations map[string]string) (time.Time, bool) {
	value := annotations[pushTimestampAnnotation]
	if value == "" {
		return time.Time{}, false
	}
	ts, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, false
	}
	return ts, true
}
