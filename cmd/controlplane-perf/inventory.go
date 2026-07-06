// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package main

import (
	"fmt"
	"io"
	"strings"

	internaljson "github.com/envoyproxy/ai-gateway/internal/json"
)

type controllerInventory struct {
	Controller           string   `json:"controller"`
	PrimaryKind          string   `json:"primaryKind"`
	Watches              []string `json:"watches"`
	Writes               []string `json:"writes"`
	Fanout               []string `json:"fanout,omitempty"`
	SuggestedObservables []string `json:"suggestedObservables,omitempty"`
}

var controllerInventoryItems = []controllerInventory{
	{
		Controller:  "GatewayController",
		PrimaryKind: "gateway.networking.k8s.io/v1 Gateway",
		Watches: []string{
			"gateway.networking.k8s.io/v1 Gateway",
			"channel event from AIGatewayRoute/MCPRoute/GatewayConfig",
		},
		Writes: []string{
			"v1 Secret (extproc filter config)",
			"v1 Pod patch (UUID annotation)",
			"apps/v1 Deployment patch (pod template UUID annotation)",
			"apps/v1 DaemonSet patch (pod template UUID annotation)",
		},
		SuggestedObservables: []string{
			"filter config Secret resourceVersion change",
			"Gateway pod annotation change",
			"Gateway Deployment/DaemonSet template annotation change",
		},
	},
	{
		Controller:  "AIGatewayRouteController",
		PrimaryKind: "aigateway.envoyproxy.io/v1beta1 AIGatewayRoute",
		Watches: []string{
			"aigateway.envoyproxy.io/v1beta1 AIGatewayRoute",
		},
		Writes: []string{
			"gateway.networking.k8s.io/v1 HTTPRoute",
			"gateway.envoyproxy.io/v1alpha1 HTTPRouteFilter",
			"AIGatewayRoute finalizer update",
			"AIGatewayRoute status update",
		},
		Fanout: []string{
			"channel event to GatewayController",
		},
		SuggestedObservables: []string{
			"AIGatewayRoute status update",
			"generated HTTPRoute resourceVersion change",
			"generated HTTPRouteFilter existence",
			"Gateway filter config Secret resourceVersion change",
		},
	},
	{
		Controller:  "AIBackendController",
		PrimaryKind: "aigateway.envoyproxy.io/v1beta1 AIServiceBackend",
		Watches: []string{
			"aigateway.envoyproxy.io/v1beta1 AIServiceBackend",
		},
		Writes: []string{
			"AIServiceBackend finalizer update",
			"AIServiceBackend status update",
		},
		Fanout: []string{
			"channel event to AIGatewayRouteController",
		},
		SuggestedObservables: []string{
			"AIServiceBackend status update",
			"referencing AIGatewayRoute HTTPRoute resourceVersion change",
			"Gateway filter config Secret resourceVersion change",
		},
	},
	{
		Controller:  "BackendSecurityPolicyController",
		PrimaryKind: "aigateway.envoyproxy.io/v1beta1 BackendSecurityPolicy",
		Watches: []string{
			"aigateway.envoyproxy.io/v1beta1 BackendSecurityPolicy",
			"owned/generated Secret",
		},
		Writes: []string{
			"BackendSecurityPolicy finalizer update",
			"BackendSecurityPolicy status update",
			"v1 Secret create/update for rotated/generated credentials",
		},
		Fanout: []string{
			"channel event to AIBackendController",
			"channel event to InferencePoolController",
		},
		SuggestedObservables: []string{
			"BackendSecurityPolicy status update",
			"generated credential Secret create/update",
			"referencing AIServiceBackend status/resourceVersion change",
			"referencing route/Gateway downstream updates",
		},
	},
	{
		Controller:  "InferencePoolController",
		PrimaryKind: "inference.networking.k8s.io/v1 InferencePool",
		Watches: []string{
			"inference.networking.k8s.io/v1 InferencePool",
			"gateway.networking.k8s.io/v1 Gateway",
			"aigateway.envoyproxy.io/v1beta1 AIGatewayRoute",
			"gateway.networking.k8s.io/v1 HTTPRoute",
		},
		Writes: []string{
			"InferencePool status update",
		},
		SuggestedObservables: []string{
			"InferencePool status.parents update",
		},
	},
	{
		Controller:  "SecretController",
		PrimaryKind: "v1 Secret",
		Watches: []string{
			"v1 Secret",
		},
		Writes: []string{},
		Fanout: []string{
			"channel event to BackendSecurityPolicyController",
			"channel event to MCPRouteController",
		},
		SuggestedObservables: []string{
			"referencing BackendSecurityPolicy/MCPRoute downstream updates",
		},
	},
	{
		Controller:  "MCPRouteController",
		PrimaryKind: "aigateway.envoyproxy.io/v1beta1 MCPRoute",
		Watches: []string{
			"aigateway.envoyproxy.io/v1beta1 MCPRoute",
		},
		Writes: []string{
			"gateway.networking.k8s.io/v1 HTTPRoute (main/per-backend)",
			"gateway.envoyproxy.io/v1alpha1 Backend",
			"gateway.envoyproxy.io/v1alpha1 HTTPRouteFilter",
			"gateway.envoyproxy.io/v1alpha1 SecurityPolicy",
			"gateway.envoyproxy.io/v1alpha1 BackendTrafficPolicy",
			"v1 Secret create/update/delete for per-backend credentials",
			"MCPRoute finalizer update",
			"MCPRoute status update",
		},
		Fanout: []string{
			"channel event to GatewayController",
		},
		SuggestedObservables: []string{
			"MCPRoute status update",
			"generated main HTTPRoute resourceVersion change",
			"generated per-backend HTTPRoute resourceVersion change",
			"generated Backend/HTTPRouteFilter/SecurityPolicy/BackendTrafficPolicy resourceVersion change",
			"generated credential Secret create/update",
			"Gateway filter config Secret resourceVersion change",
		},
	},
	{
		Controller:  "GatewayConfigController",
		PrimaryKind: "aigateway.envoyproxy.io/v1beta1 GatewayConfig",
		Watches: []string{
			"aigateway.envoyproxy.io/v1beta1 GatewayConfig",
		},
		Writes: []string{
			"GatewayConfig status update",
		},
		Fanout: []string{
			"channel event to GatewayController",
		},
		SuggestedObservables: []string{
			"GatewayConfig status update",
			"Gateway filter config Secret resourceVersion change",
			"Gateway pod/deployment/daemonset annotation change",
		},
	},
	{
		Controller:  "QuotaPolicyController",
		PrimaryKind: "aigateway.envoyproxy.io/v1alpha1 QuotaPolicy",
		Watches: []string{
			"aigateway.envoyproxy.io/v1alpha1 QuotaPolicy",
			"aigateway.envoyproxy.io/v1beta1 AIServiceBackend",
		},
		Writes: []string{
			"QuotaPolicy finalizer update",
			"QuotaPolicy status update",
			"in-memory RLS xDS snapshot publish",
		},
		Fanout: []string{
			"channel event to AIGatewayRouteController",
		},
		SuggestedObservables: []string{
			"QuotaPolicy status update",
			"referencing AIGatewayRoute/HTTPRoute resourceVersion change",
			"Gateway filter config Secret resourceVersion change (quota cost expressions)",
			"optional end-to-end quota probe for final Envoy/RLS readiness",
		},
	},
	{
		Controller:  "ReferenceGrantController",
		PrimaryKind: "gateway.networking.k8s.io/v1beta1 ReferenceGrant",
		Watches: []string{
			"gateway.networking.k8s.io/v1beta1 ReferenceGrant",
		},
		Writes: []string{},
		Fanout: []string{
			"channel event to AIGatewayRouteController",
		},
		SuggestedObservables: []string{
			"referencing AIGatewayRoute/HTTPRoute resourceVersion change",
			"Gateway filter config Secret resourceVersion change",
		},
	},
}

func writeInventory(w io.Writer, format string) error {
	switch strings.ToLower(format) {
	case "json":
		payload, err := internaljson.Marshal(controllerInventoryItems)
		if err != nil {
			return fmt.Errorf("marshal inventory: %w", err)
		}
		_, err = fmt.Fprintln(w, string(payload))
		return err
	case "table", "":
		for _, item := range controllerInventoryItems {
			if _, err := fmt.Fprintf(w, "%s\n  primary: %s\n", item.Controller, item.PrimaryKind); err != nil {
				return err
			}
			if _, err := fmt.Fprintf(w, "  watches: %s\n", strings.Join(item.Watches, "; ")); err != nil {
				return err
			}
			if len(item.Writes) > 0 {
				if _, err := fmt.Fprintf(w, "  writes: %s\n", strings.Join(item.Writes, "; ")); err != nil {
					return err
				}
			}
			if len(item.Fanout) > 0 {
				if _, err := fmt.Fprintf(w, "  fanout: %s\n", strings.Join(item.Fanout, "; ")); err != nil {
					return err
				}
			}
			if len(item.SuggestedObservables) > 0 {
				if _, err := fmt.Fprintf(w, "  suggested hops: %s\n", strings.Join(item.SuggestedObservables, "; ")); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprintln(w); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported inventory format %q", format)
	}
}
