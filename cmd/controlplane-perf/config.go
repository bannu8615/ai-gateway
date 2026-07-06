// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package main

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"text/template"
	"time"

	"sigs.k8s.io/yaml"
)

type planFile struct {
	Vars      map[string]string `json:"vars,omitempty" yaml:"vars,omitempty"`
	Defaults  planDefaults      `json:"defaults,omitempty" yaml:"defaults,omitempty"`
	Scenarios []scenarioPlan    `json:"scenarios" yaml:"scenarios"`
}

type planDefaults struct {
	PollInterval        string  `json:"pollInterval,omitempty" yaml:"pollInterval,omitempty"`
	TargetTimeout       string  `json:"targetTimeout,omitempty" yaml:"targetTimeout,omitempty"`
	CreateConcurrency   int     `json:"createConcurrency,omitempty" yaml:"createConcurrency,omitempty"`
	SuccessThreshold    float64 `json:"successThreshold,omitempty" yaml:"successThreshold,omitempty"`
	SettlePrerequisites string  `json:"settlePrerequisites,omitempty" yaml:"settlePrerequisites,omitempty"`
	SettleCompanions    string  `json:"settleCompanions,omitempty" yaml:"settleCompanions,omitempty"`
}

type scenarioPlan struct {
	Name                string             `json:"name" yaml:"name"`
	Description         string             `json:"description,omitempty" yaml:"description,omitempty"`
	Enabled             *bool              `json:"enabled,omitempty" yaml:"enabled,omitempty"`
	SkipIfMissing       bool               `json:"skipIfMissing,omitempty" yaml:"skipIfMissing,omitempty"`
	CreateConcurrency   int                `json:"createConcurrency,omitempty" yaml:"createConcurrency,omitempty"`
	SuccessThreshold    float64            `json:"successThreshold,omitempty" yaml:"successThreshold,omitempty"`
	SettlePrerequisites string             `json:"settlePrerequisites,omitempty" yaml:"settlePrerequisites,omitempty"`
	SettleCompanions    string             `json:"settleCompanions,omitempty" yaml:"settleCompanions,omitempty"`
	Ramp                rampPlan           `json:"ramp" yaml:"ramp"`
	Prerequisites       []manifestTemplate `json:"prerequisites,omitempty" yaml:"prerequisites,omitempty"`
	Companions          []manifestTemplate `json:"companions,omitempty" yaml:"companions,omitempty"`
	Source              manifestTemplate   `json:"source" yaml:"source"`
	Hops                []hopPlan          `json:"hops,omitempty" yaml:"hops,omitempty"`
}

type rampPlan struct {
	Start         int  `json:"start,omitempty" yaml:"start,omitempty"`
	Step          int  `json:"step,omitempty" yaml:"step,omitempty"`
	Max           int  `json:"max" yaml:"max"`
	StopOnFailure bool `json:"stopOnFailure,omitempty" yaml:"stopOnFailure,omitempty"`
}

type manifestTemplate struct {
	Manifest string `json:"manifest" yaml:"manifest"`
	Apply    string `json:"apply,omitempty" yaml:"apply,omitempty"`
}

type hopPlan struct {
	ID          string            `json:"id" yaml:"id"`
	Description string            `json:"description,omitempty" yaml:"description,omitempty"`
	Scope       string            `json:"scope,omitempty" yaml:"scope,omitempty"`
	Object      objectRefTemplate `json:"object" yaml:"object"`
	Condition   hopCondition      `json:"condition" yaml:"condition"`
}

type objectRefTemplate struct {
	APIVersion string `json:"apiVersion" yaml:"apiVersion"`
	Kind       string `json:"kind" yaml:"kind"`
	Namespace  string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	Name       string `json:"name,omitempty" yaml:"name,omitempty"`
}

type hopCondition struct {
	Type       string `json:"type" yaml:"type"`
	Condition  string `json:"condition,omitempty" yaml:"condition,omitempty"`
	Status     string `json:"status,omitempty" yaml:"status,omitempty"`
	Annotation string `json:"annotation,omitempty" yaml:"annotation,omitempty"`
}

type templateData struct {
	RunID    string
	Scenario string
	Wave     int
	Index    int
	Name     string
	Vars     map[string]string
}

func loadPlan(path string) (*planFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read plan: %w", err)
	}
	var plan planFile
	if err = yaml.UnmarshalStrict(data, &plan); err != nil {
		return nil, fmt.Errorf("parse plan: %w", err)
	}
	if len(plan.Scenarios) == 0 {
		return nil, fmt.Errorf("plan must include at least one scenario")
	}
	if plan.Vars == nil {
		plan.Vars = map[string]string{}
	}
	return &plan, nil
}

func parseDuration(value string, fallback time.Duration) (time.Duration, error) {
	if strings.TrimSpace(value) == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse duration %q: %w", value, err)
	}
	return d, nil
}

func renderTemplateString(raw string, data templateData) (string, error) {
	tmpl, err := template.New("manifest").Option("missingkey=error").Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parse template: %w", err)
	}
	var out bytes.Buffer
	if err = tmpl.Execute(&out, data); err != nil {
		return "", fmt.Errorf("execute template: %w", err)
	}
	return out.String(), nil
}

func mergeVars(base map[string]string, overrides []string) (map[string]string, error) {
	result := make(map[string]string, len(base)+len(overrides))
	for k, v := range base {
		result[k] = v
	}
	for _, item := range overrides {
		key, value, ok := strings.Cut(item, "=")
		if !ok || key == "" {
			return nil, fmt.Errorf("invalid --set value %q, expected key=value", item)
		}
		result[key] = value
	}
	return result, nil
}

func scenarioEnabled(s scenarioPlan) bool {
	return s.Enabled == nil || *s.Enabled
}

const samplePlan = `vars:
  namespace: default
  gatewayName: perf-gateway
  gatewayNamespace: default
  gatewayClassName: perf-gateway-class
  controllerNamespace: envoy-gateway-system
  backendName: perf-backend

defaults:
  pollInterval: 100ms
  targetTimeout: 2m
  createConcurrency: 10
  successThreshold: 1.0
  settlePrerequisites: 2s
  settleCompanions: 2s

scenarios:
  - name: aigatewayroute-create
    description: Create AIGatewayRoute objects and measure HTTPRoute/Gateway fanout.
    ramp:
      start: 25
      step: 25
      max: 100
      stopOnFailure: true
    prerequisites:
      - apply: upsert
        manifest: |
          apiVersion: gateway.networking.k8s.io/v1
          kind: GatewayClass
          metadata:
            name: '{{ index .Vars "gatewayClassName" }}'
          spec:
            controllerName: gateway.envoyproxy.io/gatewayclass-controller
          ---
          apiVersion: gateway.networking.k8s.io/v1
          kind: Gateway
          metadata:
            name: '{{ index .Vars "gatewayName" }}'
            namespace: '{{ index .Vars "gatewayNamespace" }}'
          spec:
            gatewayClassName: '{{ index .Vars "gatewayClassName" }}'
            listeners:
              - name: http
                protocol: HTTP
                port: 80
          ---
          apiVersion: gateway.envoyproxy.io/v1alpha1
          kind: Backend
          metadata:
            name: '{{ index .Vars "backendName" }}'
            namespace: '{{ index .Vars "namespace" }}'
          spec:
            endpoints:
              - fqdn:
                  hostname: example.invalid
                  port: 443
          ---
          apiVersion: aigateway.envoyproxy.io/v1beta1
          kind: AIServiceBackend
          metadata:
            name: '{{ index .Vars "backendName" }}'
            namespace: '{{ index .Vars "namespace" }}'
          spec:
            schema:
              name: OpenAI
              version: v1
            backendRef:
              group: gateway.envoyproxy.io
              kind: Backend
              name: '{{ index .Vars "backendName" }}'
    source:
      apply: create
      manifest: |
        apiVersion: aigateway.envoyproxy.io/v1beta1
        kind: AIGatewayRoute
        metadata:
          name: '{{ .Name }}'
          namespace: '{{ index .Vars "namespace" }}'
          labels:
            aigw-perf-run: '{{ .RunID }}'
            aigw-perf-scenario: '{{ .Scenario }}'
        spec:
          parentRefs:
            - name: '{{ index .Vars "gatewayName" }}'
              namespace: '{{ index .Vars "gatewayNamespace" }}'
              kind: Gateway
              group: gateway.networking.k8s.io
          rules:
            - matches:
                - headers:
                    - name: x-route-id
                      type: Exact
                      value: '{{ .Name }}'
              backendRefs:
                - name: '{{ index .Vars "backendName" }}'
                  weight: 1
    hops:
      - id: route-status
        object:
          apiVersion: aigateway.envoyproxy.io/v1beta1
          kind: AIGatewayRoute
          namespace: '{{ index .Vars "namespace" }}'
          name: '{{ .Name }}'
        condition:
          type: statusCondition
          condition: Accepted
          status: "True"
      - id: httproute
        object:
          apiVersion: gateway.networking.k8s.io/v1
          kind: HTTPRoute
          namespace: '{{ index .Vars "namespace" }}'
          name: '{{ .Name }}'
        condition:
          type: resourceVersionChanged
      - id: gateway-secret
        scope: wave
        object:
          apiVersion: v1
          kind: Secret
          namespace: '{{ index .Vars "controllerNamespace" }}'
          name: '{{ index .Vars "gatewayName" }}-{{ index .Vars "gatewayNamespace" }}'
        condition:
          type: resourceVersionChanged
`
