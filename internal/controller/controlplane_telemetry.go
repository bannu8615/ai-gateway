// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package controller

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"k8s.io/apimachinery/pkg/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/envoyproxy/ai-gateway/internal/controlplaneotel"
)

type telemetryRecord struct {
	parent      trace.SpanContext
	recordedAt  time.Time
	source      string
	destination string
	reason      string
}

type controllerTelemetry struct {
	provider   *controlplaneotel.Provider
	tracer     trace.Tracer
	propagator propagation.TextMapPropagator
	mu         sync.Mutex
	records    map[string]telemetryRecord
}

var controlPlaneTelemetry = newControllerTelemetry(nil)

func newControllerTelemetry(provider *controlplaneotel.Provider) *controllerTelemetry {
	tracer := noop.NewTracerProvider().Tracer("envoyproxy/ai-gateway/controller")
	propagator := propagation.NewCompositeTextMapPropagator()
	if provider != nil {
		tracer = provider.Tracer()
		propagator = provider.Propagator()
	}
	return &controllerTelemetry{
		provider:   provider,
		tracer:     tracer,
		propagator: propagator,
		records:    map[string]telemetryRecord{},
	}
}

func initializeControlPlaneTelemetry(ctx context.Context) error {
	provider, err := controlplaneotel.New(ctx, os.Stdout, "ai-gateway-controller", "envoyproxy/ai-gateway/controller")
	if err != nil {
		return err
	}
	controlPlaneTelemetry = newControllerTelemetry(provider)
	return nil
}

func shutdownControlPlaneTelemetry(ctx context.Context) error {
	return controlPlaneTelemetry.provider.Shutdown(ctx)
}

func startReconcileSpan(ctx context.Context, controllerName string, kind string, key ctrlclient.ObjectKey) (context.Context, trace.Span) {
	return controlPlaneTelemetry.startReconcileSpan(ctx, controllerName, kind, key)
}

func emitGenericEvent(ctx context.Context, ch chan event.GenericEvent, obj ctrlclient.Object, source string, destination string, reason string) {
	controlPlaneTelemetry.enqueue(ctx, obj, source, destination, reason)
	ch <- event.GenericEvent{Object: obj}
}

func enqueueMappedRequest(ctx context.Context, sourceObj ctrlclient.Object, destinationKind string, key ctrlclient.ObjectKey, source string, destination string, reason string) {
	controlPlaneTelemetry.enqueueRequest(ctx, sourceObj, destinationKind, key, source, destination, reason)
}

func (t *controllerTelemetry) wrapClient(base ctrlclient.Client) ctrlclient.Client {
	if base == nil {
		return nil
	}
	return &telemetryClient{Client: base, telemetry: t}
}

func (t *controllerTelemetry) watchPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			t.recordPrimaryWatchEvent(e.Object)
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			t.recordPrimaryWatchEvent(e.ObjectNew)
			return true
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			t.recordPrimaryWatchEvent(e.Object)
			return true
		},
		GenericFunc: func(e event.GenericEvent) bool {
			t.recordPrimaryWatchEvent(e.Object)
			return true
		},
	}
}

func (t *controllerTelemetry) recordPrimaryWatchEvent(obj ctrlclient.Object) {
	if obj == nil {
		return
	}
	kind := objectKind(obj)
	now := time.Now().UTC()
	parentCtx := controlplaneotel.Extract(t.propagator, obj.GetAnnotations())
	parentSpanContext := trace.SpanContextFromContext(parentCtx)
	if parentSpanContext.IsValid() {
		attrs := objectAttributes(kind, obj.GetNamespace(), obj.GetName())
		if pushTime, ok := controlplaneotel.PushTimestamp(obj.GetAnnotations()); ok {
			watchCtx, span := t.tracer.Start(parentCtx, "controlplane.apiserver_to_watch",
				trace.WithTimestamp(pushTime),
				trace.WithAttributes(attrs...),
			)
			span.End(trace.WithTimestamp(now))
			parentSpanContext = trace.SpanContextFromContext(watchCtx)
		}
	}
	t.store(kind, obj.GetNamespace(), obj.GetName(), telemetryRecord{
		parent:      parentSpanContext,
		recordedAt:  now,
		source:      "apiserver",
		destination: kind,
		reason:      "watch",
	})
}

func (t *controllerTelemetry) enqueue(ctx context.Context, obj ctrlclient.Object, source string, destination string, reason string) {
	if obj == nil {
		return
	}
	kind := objectKind(obj)
	parentCtx := ctx
	if !trace.SpanContextFromContext(parentCtx).IsValid() {
		parentCtx = controlplaneotel.Extract(t.propagator, obj.GetAnnotations())
	}
	parentSpanContext := trace.SpanContextFromContext(parentCtx)
	if parentSpanContext.IsValid() {
		_, span := t.tracer.Start(parentCtx, "controlplane.enqueue",
			trace.WithAttributes(append(objectAttributes(kind, obj.GetNamespace(), obj.GetName()),
				attribute.String("controlplane.source", source),
				attribute.String("controlplane.destination", destination),
				attribute.String("controlplane.reason", reason),
			)...),
		)
		parentSpanContext = span.SpanContext()
		span.End()
	}
	t.store(kind, obj.GetNamespace(), obj.GetName(), telemetryRecord{
		parent:      parentSpanContext,
		recordedAt:  time.Now().UTC(),
		source:      source,
		destination: destination,
		reason:      reason,
	})
}

func (t *controllerTelemetry) enqueueRequest(ctx context.Context, sourceObj ctrlclient.Object, destinationKind string, key ctrlclient.ObjectKey, source string, destination string, reason string) {
	parentCtx := ctx
	if !trace.SpanContextFromContext(parentCtx).IsValid() && sourceObj != nil {
		parentCtx = controlplaneotel.Extract(t.propagator, sourceObj.GetAnnotations())
	}
	parentSpanContext := trace.SpanContextFromContext(parentCtx)
	if parentSpanContext.IsValid() {
		_, span := t.tracer.Start(parentCtx, "controlplane.enqueue",
			trace.WithAttributes(append(objectAttributes(destinationKind, key.Namespace, key.Name),
				attribute.String("controlplane.source", source),
				attribute.String("controlplane.destination", destination),
				attribute.String("controlplane.reason", reason),
			)...),
		)
		parentSpanContext = span.SpanContext()
		span.End()
	}
	t.store(destinationKind, key.Namespace, key.Name, telemetryRecord{
		parent:      parentSpanContext,
		recordedAt:  time.Now().UTC(),
		source:      source,
		destination: destination,
		reason:      reason,
	})
}

func (t *controllerTelemetry) startReconcileSpan(ctx context.Context, controllerName string, kind string, key ctrlclient.ObjectKey) (context.Context, trace.Span) {
	now := time.Now().UTC()
	parentCtx := ctx
	if record, ok := t.take(kind, key.Namespace, key.Name); ok {
		if record.parent.IsValid() {
			parentCtx = trace.ContextWithSpanContext(context.Background(), record.parent)
		}
		deliveryAttrs := append(objectAttributes(kind, key.Namespace, key.Name),
			attribute.String("controlplane.source", record.source),
			attribute.String("controlplane.destination", controllerName),
			attribute.String("controlplane.reason", record.reason),
		)
		deliveryCtx, deliverySpan := t.tracer.Start(parentCtx, "controlplane.delivery",
			trace.WithTimestamp(record.recordedAt),
			trace.WithAttributes(deliveryAttrs...),
		)
		deliverySpan.End(trace.WithTimestamp(now))
		parentCtx = deliveryCtx
	}
	return t.tracer.Start(parentCtx, "controlplane.reconcile",
		trace.WithAttributes(append(objectAttributes(kind, key.Namespace, key.Name),
			attribute.String("controlplane.controller", controllerName),
		)...),
	)
}

func (t *controllerTelemetry) traceObjectWrite(ctx context.Context, spanName string, operation string, obj ctrlclient.Object, fn func() error, propagate bool) error {
	if propagate {
		t.inject(ctx, obj)
	}
	ctx, span := t.tracer.Start(ctx, spanName, trace.WithAttributes(append(objectAttributes(objectKind(obj), obj.GetNamespace(), obj.GetName()),
		attribute.String("controlplane.operation", operation),
	)...))
	defer span.End()
	err := fn()
	if err != nil {
		recordTraceError(span, err)
	}
	_ = ctx
	return err
}

func (t *controllerTelemetry) traceNamedOperation(ctx context.Context, spanName string, operation string, kind string, namespace string, name string, fn func() error) error {
	_, span := t.tracer.Start(ctx, spanName, trace.WithAttributes(append(objectAttributes(kind, namespace, name),
		attribute.String("controlplane.operation", operation),
	)...))
	defer span.End()
	err := fn()
	if err != nil {
		recordTraceError(span, err)
	}
	return err
}

func (t *controllerTelemetry) inject(ctx context.Context, obj ctrlclient.Object) {
	if obj == nil || !trace.SpanContextFromContext(ctx).IsValid() {
		return
	}
	obj.SetAnnotations(controlplaneotel.Inject(ctx, t.propagator, obj.GetAnnotations(), time.Now().UTC()))
}

func (t *controllerTelemetry) store(kind string, namespace string, name string, record telemetryRecord) {
	if kind == "" || name == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.records[telemetryKey(kind, namespace, name)] = record
}

func (t *controllerTelemetry) take(kind string, namespace string, name string) (telemetryRecord, bool) {
	if kind == "" || name == "" {
		return telemetryRecord{}, false
	}
	key := telemetryKey(kind, namespace, name)
	t.mu.Lock()
	defer t.mu.Unlock()
	record, ok := t.records[key]
	if ok {
		delete(t.records, key)
	}
	return record, ok
}

func telemetryKey(kind string, namespace string, name string) string {
	return fmt.Sprintf("%s/%s/%s", kind, namespace, name)
}

func objectKind(obj ctrlclient.Object) string {
	if obj == nil {
		return ""
	}
	if kind := obj.GetObjectKind().GroupVersionKind().Kind; kind != "" {
		return kind
	}
	typ := reflect.TypeOf(obj)
	for typ != nil && typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ == nil {
		return ""
	}
	return typ.Name()
}

func objectAttributes(kind string, namespace string, name string) []attribute.KeyValue {
	attrs := []attribute.KeyValue{}
	if kind != "" {
		attrs = append(attrs, attribute.String("k8s.kind", kind))
	}
	if namespace != "" {
		attrs = append(attrs, attribute.String("k8s.namespace.name", namespace))
	}
	if name != "" {
		attrs = append(attrs, attribute.String("k8s.object.name", name))
	}
	return attrs
}

func recordTraceError(span trace.Span, err error) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}

type telemetryClient struct {
	ctrlclient.Client
	telemetry *controllerTelemetry
}

func (c *telemetryClient) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...ctrlclient.ApplyOption) error {
	return c.telemetry.traceNamedOperation(ctx, "controlplane.client.apply", "apply", "", "", "", func() error {
		return c.Client.Apply(ctx, obj, opts...)
	})
}

func (c *telemetryClient) Create(ctx context.Context, obj ctrlclient.Object, opts ...ctrlclient.CreateOption) error {
	return c.telemetry.traceObjectWrite(ctx, "controlplane.client.create", "create", obj, func() error {
		return c.Client.Create(ctx, obj, opts...)
	}, true)
}

func (c *telemetryClient) Delete(ctx context.Context, obj ctrlclient.Object, opts ...ctrlclient.DeleteOption) error {
	return c.telemetry.traceObjectWrite(ctx, "controlplane.client.delete", "delete", obj, func() error {
		return c.Client.Delete(ctx, obj, opts...)
	}, false)
}

func (c *telemetryClient) Update(ctx context.Context, obj ctrlclient.Object, opts ...ctrlclient.UpdateOption) error {
	return c.telemetry.traceObjectWrite(ctx, "controlplane.client.update", "update", obj, func() error {
		return c.Client.Update(ctx, obj, opts...)
	}, true)
}

func (c *telemetryClient) Patch(ctx context.Context, obj ctrlclient.Object, patch ctrlclient.Patch, opts ...ctrlclient.PatchOption) error {
	return c.telemetry.traceObjectWrite(ctx, "controlplane.client.patch", "patch", obj, func() error {
		return c.Client.Patch(ctx, obj, patch, opts...)
	}, true)
}

func (c *telemetryClient) DeleteAllOf(ctx context.Context, obj ctrlclient.Object, opts ...ctrlclient.DeleteAllOfOption) error {
	return c.telemetry.traceObjectWrite(ctx, "controlplane.client.delete_all_of", "deleteAllOf", obj, func() error {
		return c.Client.DeleteAllOf(ctx, obj, opts...)
	}, false)
}

func (c *telemetryClient) Status() ctrlclient.SubResourceWriter {
	return &telemetrySubResourceWriter{SubResourceWriter: c.Client.Status(), telemetry: c.telemetry, subresource: "status"}
}

func (c *telemetryClient) SubResource(subResource string) ctrlclient.SubResourceClient {
	return &telemetrySubResourceClient{SubResourceClient: c.Client.SubResource(subResource), telemetry: c.telemetry, subresource: subResource}
}

type telemetrySubResourceWriter struct {
	ctrlclient.SubResourceWriter
	telemetry   *controllerTelemetry
	subresource string
}

func (w *telemetrySubResourceWriter) Create(ctx context.Context, obj ctrlclient.Object, subResource ctrlclient.Object, opts ...ctrlclient.SubResourceCreateOption) error {
	return w.telemetry.traceObjectWrite(ctx, "controlplane.client.subresource.create", "subresourceCreate", obj, func() error {
		return w.SubResourceWriter.Create(ctx, obj, subResource, opts...)
	}, false)
}

func (w *telemetrySubResourceWriter) Update(ctx context.Context, obj ctrlclient.Object, opts ...ctrlclient.SubResourceUpdateOption) error {
	return w.telemetry.traceObjectWrite(ctx, "controlplane.client.subresource.update", "subresourceUpdate", obj, func() error {
		return w.SubResourceWriter.Update(ctx, obj, opts...)
	}, false)
}

func (w *telemetrySubResourceWriter) Patch(ctx context.Context, obj ctrlclient.Object, patch ctrlclient.Patch, opts ...ctrlclient.SubResourcePatchOption) error {
	return w.telemetry.traceObjectWrite(ctx, "controlplane.client.subresource.patch", "subresourcePatch", obj, func() error {
		return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
	}, false)
}

func (w *telemetrySubResourceWriter) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...ctrlclient.SubResourceApplyOption) error {
	return w.telemetry.traceNamedOperation(ctx, "controlplane.client.subresource.apply", "subresourceApply", w.subresource, "", "", func() error {
		return w.SubResourceWriter.Apply(ctx, obj, opts...)
	})
}

type telemetrySubResourceClient struct {
	ctrlclient.SubResourceClient
	telemetry   *controllerTelemetry
	subresource string
}

func (c *telemetrySubResourceClient) Create(ctx context.Context, obj ctrlclient.Object, subResource ctrlclient.Object, opts ...ctrlclient.SubResourceCreateOption) error {
	return c.telemetry.traceObjectWrite(ctx, "controlplane.client.subresource.create", "subresourceCreate", obj, func() error {
		return c.SubResourceClient.Create(ctx, obj, subResource, opts...)
	}, false)
}

func (c *telemetrySubResourceClient) Update(ctx context.Context, obj ctrlclient.Object, opts ...ctrlclient.SubResourceUpdateOption) error {
	return c.telemetry.traceObjectWrite(ctx, "controlplane.client.subresource.update", "subresourceUpdate", obj, func() error {
		return c.SubResourceClient.Update(ctx, obj, opts...)
	}, false)
}

func (c *telemetrySubResourceClient) Patch(ctx context.Context, obj ctrlclient.Object, patch ctrlclient.Patch, opts ...ctrlclient.SubResourcePatchOption) error {
	return c.telemetry.traceObjectWrite(ctx, "controlplane.client.subresource.patch", "subresourcePatch", obj, func() error {
		return c.SubResourceClient.Patch(ctx, obj, patch, opts...)
	}, false)
}

func (c *telemetrySubResourceClient) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...ctrlclient.SubResourceApplyOption) error {
	return c.telemetry.traceNamedOperation(ctx, "controlplane.client.subresource.apply", "subresourceApply", c.subresource, "", "", func() error {
		return c.SubResourceClient.Apply(ctx, obj, opts...)
	})
}
