// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	internaljson "github.com/envoyproxy/ai-gateway/internal/json"
)

func TestLoadPlanSamplePlan(t *testing.T) {
	path := filepath.Join(t.TempDir(), "plan.yaml")
	if err := os.WriteFile(path, []byte(samplePlan), 0o644); err != nil {
		t.Fatalf("write plan: %v", err)
	}
	plan, err := loadPlan(path)
	if err != nil {
		t.Fatalf("load plan: %v", err)
	}
	if len(plan.Scenarios) != 1 {
		t.Fatalf("expected one scenario, got %d", len(plan.Scenarios))
	}
	if plan.Scenarios[0].Name != "aigatewayroute-create" {
		t.Fatalf("unexpected scenario name %q", plan.Scenarios[0].Name)
	}
}

func TestGeneratedNameIsDNSLabel(t *testing.T) {
	name := generatedName("QuotaPolicy Controller", "20250101t010101.123456789", 2, 17)
	if len(name) == 0 || len(name) > 63 {
		t.Fatalf("unexpected length for %q", name)
	}
	if strings.HasPrefix(name, "-") || strings.HasSuffix(name, "-") {
		t.Fatalf("name has invalid edge dash: %q", name)
	}
	for _, r := range name {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' {
			t.Fatalf("name contains invalid rune %q in %q", r, name)
		}
	}
}

func TestBuildDurationSummary(t *testing.T) {
	summary := buildDurationSummary(durationSummary{Count: 3, Successes: 3}, []float64{10, 20, 30})
	if summary.MinMillis != 10 {
		t.Fatalf("expected min 10, got %v", summary.MinMillis)
	}
	if summary.P50Millis != 20 {
		t.Fatalf("expected p50 20, got %v", summary.P50Millis)
	}
	if summary.MaxMillis != 30 {
		t.Fatalf("expected max 30, got %v", summary.MaxMillis)
	}
	if summary.MeanMillis != 20 {
		t.Fatalf("expected mean 20, got %v", summary.MeanMillis)
	}
}

func TestWriteInventoryJSON(t *testing.T) {
	var buf bytes.Buffer
	if err := writeInventory(&buf, "json"); err != nil {
		t.Fatalf("write inventory: %v", err)
	}
	var items []controllerInventory
	if err := internaljson.Unmarshal(buf.Bytes(), &items); err != nil {
		t.Fatalf("unmarshal inventory: %v", err)
	}
	if len(items) == 0 {
		t.Fatal("expected inventory items")
	}
	foundQuota := false
	for _, item := range items {
		if item.Controller == "QuotaPolicyController" {
			foundQuota = true
			break
		}
	}
	if !foundQuota {
		t.Fatal("QuotaPolicyController missing from inventory")
	}
}
