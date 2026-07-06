// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package main

import (
	"context"
	"fmt"
	"io"
	"maps"
	"strings"
	"sync"
	"time"

	internaljson "github.com/envoyproxy/ai-gateway/internal/json"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	apiyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/discovery"
	memcache "k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/utils/ptr"
)

type kubeFlags struct {
	Kubeconfig string
	Context    string
	QPS        float32
	Burst      int
}

type clusterClients struct {
	dynamic dynamic.Interface
	mapper  apimeta.RESTMapper
	hub     *watchHub
}

type observedState struct {
	exists       bool
	resourceVers string
	annotations  map[string]string
	object       *unstructured.Unstructured
	observedAt   time.Time
}

type watchHub struct {
	ctx      context.Context
	dynamic  dynamic.Interface
	mapper   apimeta.RESTMapper
	mu       sync.Mutex
	watchers map[string]*resourceWatcher
}

type resourceWatcher struct {
	gvr       schema.GroupVersionResource
	namespace string
	factory   dynamicinformer.DynamicSharedInformerFactory
	informer  cache.SharedIndexInformer
	synced    chan struct{}
	syncErr   error
	mu        sync.RWMutex
	objects   map[string]observedState
}

func newClusterClients(ctx context.Context, flags kubeFlags) (*clusterClients, error) {
	cfg, err := loadRESTConfig(flags)
	if err != nil {
		return nil, err
	}
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create dynamic client: %w", err)
	}
	disco, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create discovery client: %w", err)
	}
	mapper := restmapper.NewDeferredDiscoveryRESTMapper(memcache.NewMemCacheClient(disco))
	return &clusterClients{
		dynamic: dyn,
		mapper:  mapper,
		hub: &watchHub{
			ctx:      ctx,
			dynamic:  dyn,
			mapper:   mapper,
			watchers: map[string]*resourceWatcher{},
		},
	}, nil
}

func loadRESTConfig(flags kubeFlags) (*rest.Config, error) {
	loadingRules := &clientcmd.ClientConfigLoadingRules{ExplicitPath: flags.Kubeconfig}
	overrides := &clientcmd.ConfigOverrides{}
	if flags.Context != "" {
		overrides.CurrentContext = flags.Context
	}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides).ClientConfig()
	if err != nil {
		cfg, err = rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("load kube config: %w", err)
		}
	}
	if flags.QPS > 0 {
		cfg.QPS = flags.QPS
	}
	if flags.Burst > 0 {
		cfg.Burst = flags.Burst
	}
	return cfg, nil
}

func (c *clusterClients) applyManifest(ctx context.Context, manifest string, mode string) ([]*unstructured.Unstructured, error) {
	objects, err := decodeManifest(manifest)
	if err != nil {
		return nil, err
	}
	applied := make([]*unstructured.Unstructured, 0, len(objects))
	for _, obj := range objects {
		created, createErr := c.applyObject(ctx, obj, mode)
		if createErr != nil {
			return nil, createErr
		}
		applied = append(applied, created)
	}
	return applied, nil
}

func (c *clusterClients) createSingleObject(ctx context.Context, manifest string) (*unstructured.Unstructured, error) {
	objects, err := decodeManifest(manifest)
	if err != nil {
		return nil, err
	}
	if len(objects) != 1 {
		return nil, fmt.Errorf("source manifest must decode to exactly one object, got %d", len(objects))
	}
	return c.applyObject(ctx, objects[0], "create")
}

func (c *clusterClients) applyObject(ctx context.Context, obj *unstructured.Unstructured, mode string) (*unstructured.Unstructured, error) {
	mapping, err := c.mappingForGVK(obj.GetAPIVersion(), obj.GetKind())
	if err != nil {
		return nil, err
	}
	resource := c.dynamic.Resource(mapping.Resource)
	var namespaced dynamic.ResourceInterface
	if mapping.Scope.Name() == apimeta.RESTScopeNameNamespace {
		if obj.GetNamespace() == "" {
			return nil, fmt.Errorf("object %s/%s is namespaced but manifest omitted namespace", obj.GetAPIVersion(), obj.GetKind())
		}
		namespaced = resource.Namespace(obj.GetNamespace())
	} else {
		namespaced = resource
	}
	switch normalizedApplyMode(mode) {
	case applyModeCreate:
		created, createErr := namespaced.Create(ctx, obj, metav1.CreateOptions{})
		if createErr != nil {
			return nil, fmt.Errorf("create %s %s/%s: %w", obj.GetKind(), obj.GetNamespace(), obj.GetName(), createErr)
		}
		return created, nil
	case applyModeUpsert:
		payload, marshalErr := internaljson.Marshal(obj.Object)
		if marshalErr != nil {
			return nil, fmt.Errorf("marshal apply payload for %s %s/%s: %w", obj.GetKind(), obj.GetNamespace(), obj.GetName(), marshalErr)
		}
		patched, patchErr := namespaced.Patch(ctx, obj.GetName(), types.ApplyPatchType, payload, metav1.PatchOptions{
			FieldManager: "controlplane-perf",
			Force:        ptr.To(true),
		})
		if patchErr != nil {
			return nil, fmt.Errorf("apply %s %s/%s: %w", obj.GetKind(), obj.GetNamespace(), obj.GetName(), patchErr)
		}
		return patched, nil
	default:
		return nil, fmt.Errorf("unsupported apply mode %q", mode)
	}
}

func (c *clusterClients) mappingForGVK(apiVersion string, kind string) (*apimeta.RESTMapping, error) {
	gv, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		return nil, fmt.Errorf("parse apiVersion %q: %w", apiVersion, err)
	}
	mapping, err := c.mapper.RESTMapping(schema.GroupKind{Group: gv.Group, Kind: kind}, gv.Version)
	if err != nil {
		return nil, fmt.Errorf("map %s %s: %w", apiVersion, kind, err)
	}
	return mapping, nil
}

func decodeManifest(manifest string) ([]*unstructured.Unstructured, error) {
	decoder := apiyaml.NewYAMLOrJSONDecoder(strings.NewReader(manifest), 4096)
	var objects []*unstructured.Unstructured
	for {
		var raw map[string]interface{}
		err := decoder.Decode(&raw)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("decode manifest: %w", err)
		}
		if len(raw) == 0 {
			continue
		}
		obj := &unstructured.Unstructured{Object: raw}
		if obj.GetAPIVersion() == "" || obj.GetKind() == "" {
			return nil, fmt.Errorf("manifest object missing apiVersion or kind")
		}
		objects = append(objects, obj)
	}
	return objects, nil
}

func normalizedApplyMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "create":
		return applyModeCreate
	case "apply", "upsert":
		return applyModeUpsert
	default:
		return strings.ToLower(strings.TrimSpace(mode))
	}
}

const (
	applyModeCreate = "create"
	applyModeUpsert = "upsert"
)

func (h *watchHub) ensureWatcher(ctx context.Context, ref objectRefTemplate) (*resourceWatcher, error) {
	mapping, err := h.mappingForGVK(ref.APIVersion, ref.Kind)
	if err != nil {
		return nil, err
	}
	key := watcherKey(mapping.Resource, ref.Namespace)
	h.mu.Lock()
	defer h.mu.Unlock()
	if existing := h.watchers[key]; existing != nil {
		if err = existing.waitForSync(ctx); err != nil {
			return nil, err
		}
		return existing, nil
	}
	factory := dynamicinformer.NewFilteredDynamicSharedInformerFactory(h.dynamic, 0, ref.Namespace, nil)
	informer := factory.ForResource(mapping.Resource).Informer()
	watcher := &resourceWatcher{
		gvr:       mapping.Resource,
		namespace: ref.Namespace,
		factory:   factory,
		informer:  informer,
		synced:    make(chan struct{}),
		objects:   map[string]observedState{},
	}
	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			watcher.store(obj)
		},
		UpdateFunc: func(_, newObj interface{}) {
			watcher.store(newObj)
		},
		DeleteFunc: func(obj interface{}) {
			watcher.remove(obj)
		},
	})
	go watcher.start(h.ctx)
	h.watchers[key] = watcher
	if err = watcher.waitForSync(ctx); err != nil {
		return nil, err
	}
	return watcher, nil
}

func (h *watchHub) snapshot(ctx context.Context, ref objectRefTemplate) (observedState, error) {
	watcher, err := h.ensureWatcher(ctx, ref)
	if err != nil {
		return observedState{}, err
	}
	return watcher.snapshot(ref.Namespace, ref.Name), nil
}

func (h *watchHub) mappingForGVK(apiVersion string, kind string) (*apimeta.RESTMapping, error) {
	gv, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		return nil, fmt.Errorf("parse apiVersion %q: %w", apiVersion, err)
	}
	mapping, err := h.mapper.RESTMapping(schema.GroupKind{Group: gv.Group, Kind: kind}, gv.Version)
	if err != nil {
		return nil, fmt.Errorf("map %s %s: %w", apiVersion, kind, err)
	}
	return mapping, nil
}

func watcherKey(gvr schema.GroupVersionResource, namespace string) string {
	return fmt.Sprintf("%s/%s/%s/%s", gvr.Group, gvr.Version, gvr.Resource, namespace)
}

func (w *resourceWatcher) start(ctx context.Context) {
	defer close(w.synced)
	w.factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), w.informer.HasSynced) {
		w.syncErr = fmt.Errorf("watch cache for %s/%s did not sync", w.gvr.Resource, w.namespace)
	}
}

func (w *resourceWatcher) waitForSync(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-w.synced:
		return w.syncErr
	}
}

func (w *resourceWatcher) store(obj interface{}) {
	u := asUnstructured(obj)
	if u == nil {
		return
	}
	key := objectKey(u.GetNamespace(), u.GetName())
	state := observedState{
		exists:       true,
		resourceVers: u.GetResourceVersion(),
		annotations:  maps.Clone(u.GetAnnotations()),
		object:       u.DeepCopy(),
		observedAt:   time.Now(),
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.objects[key] = state
}

func (w *resourceWatcher) remove(obj interface{}) {
	u := asUnstructured(obj)
	if u == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	delete(w.objects, objectKey(u.GetNamespace(), u.GetName()))
}

func (w *resourceWatcher) snapshot(namespace string, name string) observedState {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if state, ok := w.objects[objectKey(namespace, name)]; ok {
		copied := state
		copied.annotations = maps.Clone(state.annotations)
		if state.object != nil {
			copied.object = state.object.DeepCopy()
		}
		return copied
	}
	return observedState{annotations: map[string]string{}}
}

func objectKey(namespace string, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "/" + name
}

func asUnstructured(obj interface{}) *unstructured.Unstructured {
	switch typed := obj.(type) {
	case *unstructured.Unstructured:
		return typed
	case cache.DeletedFinalStateUnknown:
		if u, ok := typed.Obj.(*unstructured.Unstructured); ok {
			return u
		}
	}
	return nil
}
