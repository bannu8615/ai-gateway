// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package main

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	internaljson "github.com/envoyproxy/ai-gateway/internal/json"
	"go.opentelemetry.io/otel/trace"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

type runSettings struct {
	PollInterval        time.Duration
	TargetTimeout       time.Duration
	CreateConcurrency   int
	SuccessThreshold    float64
	SettlePrerequisites time.Duration
	SettleCompanions    time.Duration
}

type runReport struct {
	RunID       string           `json:"runID"`
	StartedAt   time.Time        `json:"startedAt"`
	CompletedAt time.Time        `json:"completedAt"`
	Scenarios   []scenarioReport `json:"scenarios"`
}

type scenarioReport struct {
	Name          string       `json:"name"`
	Description   string       `json:"description,omitempty"`
	Skipped       bool         `json:"skipped,omitempty"`
	SkipReason    string       `json:"skipReason,omitempty"`
	BreakingPoint int          `json:"breakingPoint,omitempty"`
	Waves         []waveReport `json:"waves,omitempty"`
}

type waveReport struct {
	Wave              int                        `json:"wave"`
	Size              int                        `json:"size"`
	CumulativeSize    int                        `json:"cumulativeSize"`
	StartedAt         time.Time                  `json:"startedAt"`
	CompletedAt       time.Time                  `json:"completedAt"`
	CreateFailures    int                        `json:"createFailures"`
	SuccessfulObjects int                        `json:"successfulObjects"`
	WaveFailed        bool                       `json:"waveFailed"`
	InstanceResults   []instanceResult           `json:"instanceResults"`
	WaveHopResults    map[string]hopResult       `json:"waveHopResults,omitempty"`
	HopSummaries      map[string]durationSummary `json:"hopSummaries"`
}

type instanceResult struct {
	Name                string               `json:"name"`
	Namespace           string               `json:"namespace,omitempty"`
	CreateStartedAt     time.Time            `json:"createStartedAt"`
	CreateCompletedAt   time.Time            `json:"createCompletedAt"`
	CreateLatencyMillis float64              `json:"createLatencyMillis,omitempty"`
	CreateError         string               `json:"createError,omitempty"`
	Hops                map[string]hopResult `json:"hops,omitempty"`
	traceCtx            context.Context      `json:"-"`
	traceSpan           trace.Span           `json:"-"`
}

type hopResult struct {
	Scope          string    `json:"scope"`
	Success        bool      `json:"success"`
	ObservedAt     time.Time `json:"observedAt,omitempty"`
	DurationMillis float64   `json:"durationMillis,omitempty"`
	Error          string    `json:"error,omitempty"`
}

type durationSummary struct {
	Count      int     `json:"count"`
	Successes  int     `json:"successes"`
	Failures   int     `json:"failures"`
	MinMillis  float64 `json:"minMillis,omitempty"`
	P50Millis  float64 `json:"p50Millis,omitempty"`
	P95Millis  float64 `json:"p95Millis,omitempty"`
	MaxMillis  float64 `json:"maxMillis,omitempty"`
	MeanMillis float64 `json:"meanMillis,omitempty"`
}

type pendingTarget struct {
	Hop         hopPlan
	Ref         objectRefTemplate
	Baseline    observedState
	CapturedAt  time.Time
	CreateTime  time.Time
	Instance    *instanceResult
	WaveResults map[string]hopResult
	WaveKey     string
	WaveSpan    trace.Span
}

func executePlan(ctx context.Context, cluster *clusterClients, plan *planFile, overrides []string, telemetry *harnessTelemetry) (*runReport, error) {
	vars, err := mergeVars(plan.Vars, overrides)
	if err != nil {
		return nil, err
	}
	runID := strings.ToLower(time.Now().UTC().Format("20060102t150405.000000000"))
	ctx, span := telemetry.startRun(ctx, runID)
	defer span.End()
	report := &runReport{RunID: runID, StartedAt: time.Now().UTC()}
	for _, scenario := range plan.Scenarios {
		result, runErr := executeScenario(ctx, cluster, vars, runID, plan.Defaults, scenario, telemetry)
		if runErr != nil {
			recordSpanError(span, runErr)
			return nil, runErr
		}
		report.Scenarios = append(report.Scenarios, result)
	}
	report.CompletedAt = time.Now().UTC()
	return report, nil
}

func executeScenario(ctx context.Context, cluster *clusterClients, vars map[string]string, runID string, defaults planDefaults, scenario scenarioPlan, telemetry *harnessTelemetry) (scenarioReport, error) {
	ctx, span := telemetry.startScenario(ctx, scenario.Name)
	defer span.End()
	result := scenarioReport{Name: scenario.Name, Description: scenario.Description}
	if !scenarioEnabled(scenario) {
		result.Skipped = true
		result.SkipReason = "disabled"
		return result, nil
	}
	settings, err := scenarioSettings(defaults, scenario)
	if err != nil {
		return result, err
	}
	if scenario.SkipIfMissing {
		apiVersion, kind := gvkFromManifestTemplate(scenario.Source.Manifest)
		if _, err = cluster.mappingForGVK(apiVersion, kind); err != nil {
			result.Skipped = true
			result.SkipReason = err.Error()
			return result, nil
		}
	}
	baseData := templateData{RunID: runID, Scenario: scenario.Name, Vars: vars}
	if err = applyTemplates(ctx, cluster, scenario.Prerequisites, baseData, settings.SettlePrerequisites, telemetry); err != nil {
		return result, fmt.Errorf("apply prerequisites for %s: %w", scenario.Name, err)
	}
	if scenario.Ramp.Max <= 0 {
		return result, fmt.Errorf("scenario %s must set ramp.max > 0", scenario.Name)
	}
	step := scenario.Ramp.Step
	if step <= 0 {
		step = scenario.Ramp.Start
	}
	if step <= 0 {
		step = 1
	}
	start := scenario.Ramp.Start
	if start <= 0 {
		start = step
	}
	cumulative := 0
	nextOrdinal := 0
	for wave, size := 1, start; size <= scenario.Ramp.Max; wave, size = wave+1, size+step {
		waveResult, waveFailed, waveErr := executeWave(ctx, cluster, vars, runID, scenario, settings, wave, size, cumulative, nextOrdinal, telemetry)
		if waveErr != nil {
			return result, waveErr
		}
		result.Waves = append(result.Waves, waveResult)
		cumulative = waveResult.CumulativeSize
		nextOrdinal += size
		if waveFailed {
			result.BreakingPoint = waveResult.CumulativeSize
			if scenario.Ramp.StopOnFailure {
				break
			}
		}
	}
	return result, nil
}

func scenarioSettings(defaults planDefaults, scenario scenarioPlan) (runSettings, error) {
	pollInterval, err := parseDuration(defaults.PollInterval, 100*time.Millisecond)
	if err != nil {
		return runSettings{}, err
	}
	targetTimeout, err := parseDuration(defaults.TargetTimeout, 2*time.Minute)
	if err != nil {
		return runSettings{}, err
	}
	settlePrereqs, err := parseDuration(defaults.SettlePrerequisites, 0)
	if err != nil {
		return runSettings{}, err
	}
	settleCompanions, err := parseDuration(defaults.SettleCompanions, 0)
	if err != nil {
		return runSettings{}, err
	}
	if scenario.SettlePrerequisites != "" {
		settlePrereqs, err = parseDuration(scenario.SettlePrerequisites, settlePrereqs)
		if err != nil {
			return runSettings{}, err
		}
	}
	if scenario.SettleCompanions != "" {
		settleCompanions, err = parseDuration(scenario.SettleCompanions, settleCompanions)
		if err != nil {
			return runSettings{}, err
		}
	}
	createConcurrency := defaults.CreateConcurrency
	if createConcurrency <= 0 {
		createConcurrency = 1
	}
	if scenario.CreateConcurrency > 0 {
		createConcurrency = scenario.CreateConcurrency
	}
	successThreshold := defaults.SuccessThreshold
	if successThreshold <= 0 {
		successThreshold = 1
	}
	if scenario.SuccessThreshold > 0 {
		successThreshold = scenario.SuccessThreshold
	}
	return runSettings{
		PollInterval:        pollInterval,
		TargetTimeout:       targetTimeout,
		CreateConcurrency:   createConcurrency,
		SuccessThreshold:    successThreshold,
		SettlePrerequisites: settlePrereqs,
		SettleCompanions:    settleCompanions,
	}, nil
}

func executeWave(ctx context.Context, cluster *clusterClients, vars map[string]string, runID string, scenario scenarioPlan, settings runSettings, wave int, size int, cumulative int, startOrdinal int, telemetry *harnessTelemetry) (waveReport, bool, error) {
	ctx, waveSpan := telemetry.startWave(ctx, scenario.Name, wave, size, cumulative+size)
	defer waveSpan.End()
	waveResult := waveReport{
		Wave:           wave,
		Size:           size,
		CumulativeSize: cumulative + size,
		StartedAt:      time.Now().UTC(),
		WaveHopResults: map[string]hopResult{},
		HopSummaries:   map[string]durationSummary{},
	}
	instances := make([]instanceResult, 0, size)
	instanceData := make([]templateData, 0, size)
	for i := 0; i < size; i++ {
		ordinal := startOrdinal + i + 1
		name := generatedName(scenario.Name, runID, wave, ordinal)
		data := templateData{
			RunID:    runID,
			Scenario: scenario.Name,
			Wave:     wave,
			Index:    ordinal,
			Name:     name,
			Vars:     vars,
		}
		instanceCtx, instanceSpan := telemetry.startObject(ctx, data)
		instanceData = append(instanceData, data)
		instances = append(instances, instanceResult{Name: name, Hops: map[string]hopResult{}, traceCtx: instanceCtx, traceSpan: instanceSpan})
	}
	traceContexts := make([]context.Context, 0, len(instances))
	for i := range instances {
		traceContexts = append(traceContexts, instances[i].traceCtx)
	}
	if err := applyTemplatesForInstances(ctx, cluster, scenario.Companions, instanceData, settings.CreateConcurrency, settings.SettleCompanions, telemetry, traceContexts); err != nil {
		recordSpanError(waveSpan, err)
		finishInstanceSpans(telemetry, instances)
		return waveResult, true, fmt.Errorf("apply companions for %s wave %d: %w", scenario.Name, wave, err)
	}
	pending, prepErr := prepareTargets(ctx, cluster, scenario.Hops, instanceData, instances, waveResult.WaveHopResults, waveSpan)
	if prepErr != nil {
		recordSpanError(waveSpan, prepErr)
		finishInstanceSpans(telemetry, instances)
		return waveResult, true, prepErr
	}
	createSources(ctx, cluster, scenario.Source, settings.CreateConcurrency, instanceData, instances, telemetry)
	waitForTargets(ctx, cluster, pending, settings.TargetTimeout, settings.PollInterval, telemetry)
	finishInstanceSpans(telemetry, instances)
	waveResult.InstanceResults = instances
	waveResult.CompletedAt = time.Now().UTC()
	waveResult.CreateFailures = countCreateFailures(instances)
	waveResult.SuccessfulObjects = countSuccessfulObjects(instances, waveResult.WaveHopResults)
	waveResult.HopSummaries = summarizeWave(instances, waveResult.WaveHopResults)
	waveFailed := waveFailed(instances, waveResult.WaveHopResults, settings.SuccessThreshold)
	waveResult.WaveFailed = waveFailed
	return waveResult, waveFailed, nil
}

func applyTemplates(ctx context.Context, cluster *clusterClients, templates []manifestTemplate, data templateData, settle time.Duration, telemetry *harnessTelemetry) error {
	for _, item := range templates {
		rendered, err := renderTemplateString(item.Manifest, data)
		if err != nil {
			return err
		}
		objects, err := decodeManifest(rendered)
		if err != nil {
			return err
		}
		for _, obj := range objects {
			pushTime := time.Now().UTC()
			telemetry.annotate(ctx, obj, pushTime)
			applyCtx, applySpan := telemetry.startApply(ctx, obj, item.Apply)
			_, err = cluster.applyObject(applyCtx, obj, item.Apply)
			if err != nil {
				recordSpanError(applySpan, err)
				applySpan.End()
				return err
			}
			applySpan.End()
		}
	}
	if settle > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(settle):
		}
	}
	return nil
}

func applyTemplatesForInstances(ctx context.Context, cluster *clusterClients, templates []manifestTemplate, data []templateData, concurrency int, settle time.Duration, telemetry *harnessTelemetry, traceContexts []context.Context) error {
	if len(templates) == 0 || len(data) == 0 {
		return nil
	}
	sem := make(chan struct{}, max(concurrency, 1))
	errCh := make(chan error, 1)
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for i, item := range data {
		idx := i
		current := item
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				return
			case sem <- struct{}{}:
			}
			defer func() { <-sem }()
			applyCtx := ctx
			if idx < len(traceContexts) && traceContexts[idx] != nil {
				applyCtx = traceContexts[idx]
			}
			if err := applyTemplates(applyCtx, cluster, templates, current, 0, telemetry); err != nil {
				select {
				case errCh <- err:
				default:
				}
				cancel()
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
	}
	if settle > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(settle):
		}
	}
	return nil
}

func prepareTargets(ctx context.Context, cluster *clusterClients, hops []hopPlan, data []templateData, instances []instanceResult, waveTargets map[string]hopResult, waveSpan trace.Span) ([]pendingTarget, error) {
	pending := make([]pendingTarget, 0, len(hops)*max(len(data), 1))
	seenWave := map[string]bool{}
	for _, hop := range hops {
		scope := normalizedScope(hop.Scope)
		switch scope {
		case scopeWave:
			if seenWave[hop.ID] {
				continue
			}
			seenWave[hop.ID] = true
			ref, err := renderObjectRef(hop.Object, templateData{RunID: data[0].RunID, Scenario: data[0].Scenario, Wave: data[0].Wave, Vars: data[0].Vars})
			if err != nil {
				return nil, err
			}
			baseline, err := cluster.hub.snapshot(ctx, ref)
			if err != nil {
				return nil, err
			}
			waveTargets[hop.ID] = hopResult{Scope: scopeWave}
			pending = append(pending, pendingTarget{
				Hop:         hop,
				Ref:         ref,
				Baseline:    baseline,
				CapturedAt:  time.Now(),
				WaveResults: waveTargets,
				WaveKey:     hop.ID,
				WaveSpan:    waveSpan,
			})
		case scopeInstance:
			for i, item := range data {
				ref, err := renderObjectRef(hop.Object, item)
				if err != nil {
					return nil, err
				}
				baseline, err := cluster.hub.snapshot(ctx, ref)
				if err != nil {
					return nil, err
				}
				pending = append(pending, pendingTarget{
					Hop:        hop,
					Ref:        ref,
					Baseline:   baseline,
					CapturedAt: time.Now(),
					Instance:   &instances[i],
				})
			}
		default:
			return nil, fmt.Errorf("unsupported hop scope %q", hop.Scope)
		}
	}
	return pending, nil
}

func createSources(ctx context.Context, cluster *clusterClients, source manifestTemplate, concurrency int, data []templateData, instances []instanceResult, telemetry *harnessTelemetry) {
	sem := make(chan struct{}, max(concurrency, 1))
	var wg sync.WaitGroup
	for i := range data {
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-ctx.Done():
				return
			case sem <- struct{}{}:
			}
			defer func() { <-sem }()
			instances[idx].CreateStartedAt = time.Now().UTC()
			rendered, err := renderTemplateString(source.Manifest, data[idx])
			if err != nil {
				instances[idx].CreateCompletedAt = time.Now().UTC()
				instances[idx].CreateError = err.Error()
				telemetry.addCreateError(&instances[idx], err)
				return
			}
			objects, err := decodeManifest(rendered)
			if err != nil {
				instances[idx].CreateCompletedAt = time.Now().UTC()
				instances[idx].CreateError = err.Error()
				telemetry.addCreateError(&instances[idx], err)
				return
			}
			if len(objects) != 1 {
				err = fmt.Errorf("source manifest must decode to exactly one object, got %d", len(objects))
				instances[idx].CreateCompletedAt = time.Now().UTC()
				instances[idx].CreateError = err.Error()
				telemetry.addCreateError(&instances[idx], err)
				return
			}
			obj := objects[0]
			telemetry.annotate(instances[idx].traceCtx, obj, instances[idx].CreateStartedAt)
			applyCtx, applySpan := telemetry.startApply(instances[idx].traceCtx, obj, source.Apply)
			obj, err = cluster.applyObject(applyCtx, obj, source.Apply)
			if err != nil {
				recordSpanError(applySpan, err)
			}
			applySpan.End()
			instances[idx].CreateCompletedAt = time.Now().UTC()
			instances[idx].CreateLatencyMillis = durationMillis(instances[idx].CreateCompletedAt.Sub(instances[idx].CreateStartedAt))
			if err != nil {
				instances[idx].CreateError = err.Error()
				telemetry.addCreateError(&instances[idx], err)
				return
			}
			instances[idx].Namespace = obj.GetNamespace()
			instances[idx].traceCtx = applyCtx
		}()
	}
	wg.Wait()
}

func waitForTargets(ctx context.Context, cluster *clusterClients, pending []pendingTarget, timeout time.Duration, pollInterval time.Duration, telemetry *harnessTelemetry) {
	deadline := time.Now().Add(timeout)
	active := make([]pendingTarget, 0, len(pending))
	for _, item := range pending {
		if item.Instance != nil {
			if item.Instance.CreateError != "" {
				item.Instance.Hops[item.Hop.ID] = hopResult{Scope: scopeInstance, Error: "source create failed"}
				telemetry.addHopFailure(item.Instance, item.Hop.ID, "source create failed")
				continue
			}
			item.CreateTime = item.Instance.CreateCompletedAt
		} else {
			item.CreateTime = time.Now().UTC()
		}
		active = append(active, item)
	}
	for len(active) > 0 && time.Now().Before(deadline) {
		next := active[:0]
		for _, item := range active {
			state, err := cluster.hub.snapshot(ctx, item.Ref)
			if err != nil {
				recordHopFailure(item, err.Error(), telemetry)
				continue
			}
			if satisfied := hopSatisfied(state, item.Baseline, item.CapturedAt, item.Hop.Condition); satisfied {
				recordHopSuccess(item, state.observedAt, telemetry)
				continue
			}
			next = append(next, item)
		}
		active = next
		if len(active) == 0 {
			break
		}
		select {
		case <-ctx.Done():
			for _, item := range active {
				recordHopFailure(item, ctx.Err().Error(), telemetry)
			}
			return
		case <-time.After(pollInterval):
		}
	}
	for _, item := range active {
		recordHopFailure(item, fmt.Sprintf("timed out after %s", timeout), telemetry)
	}
}

func recordHopSuccess(item pendingTarget, observedAt time.Time, telemetry *harnessTelemetry) {
	result := hopResult{
		Scope:          normalizedScope(item.Hop.Scope),
		Success:        true,
		ObservedAt:     observedAt,
		DurationMillis: durationMillis(observedAt.Sub(item.CreateTime)),
	}
	if item.Instance != nil {
		item.Instance.Hops[item.Hop.ID] = result
		telemetry.addHopSuccess(item.Instance, item.Hop.ID, observedAt, result.DurationMillis)
		return
	}
	if item.WaveResults != nil {
		item.WaveResults[item.WaveKey] = result
		telemetry.addWaveHopSuccess(item.WaveSpan, item.Hop.ID, observedAt, result.DurationMillis)
	}
}

func recordHopFailure(item pendingTarget, message string, telemetry *harnessTelemetry) {
	result := hopResult{Scope: normalizedScope(item.Hop.Scope), Error: message}
	if item.Instance != nil {
		item.Instance.Hops[item.Hop.ID] = result
		telemetry.addHopFailure(item.Instance, item.Hop.ID, message)
		return
	}
	if item.WaveResults != nil {
		item.WaveResults[item.WaveKey] = result
		telemetry.addWaveHopFailure(item.WaveSpan, item.Hop.ID, message)
	}
}

func hopSatisfied(state observedState, baseline observedState, capturedAt time.Time, condition hopCondition) bool {
	advanced := resourceAdvanced(state, baseline, capturedAt)
	switch strings.ToLower(strings.TrimSpace(condition.Type)) {
	case "exists":
		return state.exists && state.observedAt.After(capturedAt)
	case "resourceversionchanged":
		return advanced
	case "statuscondition":
		if !advanced || state.object == nil {
			return false
		}
		return hasStatusCondition(state.object, condition.Condition, condition.Status)
	case "annotationchanged":
		if !advanced {
			return false
		}
		return state.annotations[condition.Annotation] != baseline.annotations[condition.Annotation]
	default:
		return false
	}
}

func resourceAdvanced(state observedState, baseline observedState, capturedAt time.Time) bool {
	if !state.exists || !state.observedAt.After(capturedAt) {
		return false
	}
	if baseline.resourceVers == "" {
		return true
	}
	return state.resourceVers != baseline.resourceVers
}

func hasStatusCondition(obj *unstructured.Unstructured, condType string, status string) bool {
	conditions, found, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if !found {
		return false
	}
	expectedStatus := status
	if expectedStatus == "" {
		expectedStatus = "True"
	}
	for _, item := range conditions {
		condition, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		if fmt.Sprint(condition["type"]) == condType && fmt.Sprint(condition["status"]) == expectedStatus {
			return true
		}
	}
	return false
}

func summarizeWave(instances []instanceResult, waveHops map[string]hopResult) map[string]durationSummary {
	summaries := map[string]durationSummary{}
	durations := map[string][]float64{}
	createSummary := durationSummary{Count: len(instances)}
	for _, instance := range instances {
		if instance.CreateError != "" {
			createSummary.Failures++
		} else {
			createSummary.Successes++
			durations["apiserverWrite"] = append(durations["apiserverWrite"], instance.CreateLatencyMillis)
		}
		for hopName, hop := range instance.Hops {
			summary := summaries[hopName]
			summary.Count++
			if hop.Success {
				summary.Successes++
				durations[hopName] = append(durations[hopName], hop.DurationMillis)
			} else {
				summary.Failures++
			}
			summaries[hopName] = summary
		}
	}
	summaries["apiserverWrite"] = buildDurationSummary(createSummary, durations["apiserverWrite"])
	for hopName, hop := range waveHops {
		summary := durationSummary{Count: 1}
		if hop.Success {
			summary.Successes = 1
			durations[hopName] = append(durations[hopName], hop.DurationMillis)
		} else {
			summary.Failures = 1
		}
		summaries[hopName] = buildDurationSummary(summary, durations[hopName])
	}
	for name, summary := range summaries {
		summaries[name] = buildDurationSummary(summary, durations[name])
	}
	return summaries
}

func buildDurationSummary(summary durationSummary, values []float64) durationSummary {
	if len(values) == 0 {
		return summary
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	summary.MinMillis = sorted[0]
	summary.P50Millis = percentile(sorted, 0.50)
	summary.P95Millis = percentile(sorted, 0.95)
	summary.MaxMillis = sorted[len(sorted)-1]
	for _, value := range sorted {
		summary.MeanMillis += value
	}
	summary.MeanMillis /= float64(len(sorted))
	return summary
}

func countCreateFailures(instances []instanceResult) int {
	failures := 0
	for _, instance := range instances {
		if instance.CreateError != "" {
			failures++
		}
	}
	return failures
}

func countSuccessfulObjects(instances []instanceResult, waveHops map[string]hopResult) int {
	waveHealthy := true
	for _, hop := range waveHops {
		if !hop.Success {
			waveHealthy = false
			break
		}
	}
	if !waveHealthy {
		return 0
	}
	successes := 0
	for _, instance := range instances {
		if instance.CreateError != "" {
			continue
		}
		healthy := true
		for _, hop := range instance.Hops {
			if !hop.Success {
				healthy = false
				break
			}
		}
		if healthy {
			successes++
		}
	}
	return successes
}

func waveFailed(instances []instanceResult, waveHops map[string]hopResult, threshold float64) bool {
	for _, hop := range waveHops {
		if !hop.Success {
			return true
		}
	}
	if len(instances) == 0 {
		return true
	}
	successes := countSuccessfulObjects(instances, waveHops)
	ratio := float64(successes) / float64(len(instances))
	return ratio < threshold
}

func renderObjectRef(raw objectRefTemplate, data templateData) (objectRefTemplate, error) {
	ref := objectRefTemplate{APIVersion: raw.APIVersion, Kind: raw.Kind}
	var err error
	if raw.Namespace != "" {
		ref.Namespace, err = renderTemplateString(raw.Namespace, data)
		if err != nil {
			return objectRefTemplate{}, err
		}
	}
	if raw.Name != "" {
		ref.Name, err = renderTemplateString(raw.Name, data)
		if err != nil {
			return objectRefTemplate{}, err
		}
	}
	return ref, nil
}

func generatedName(scenario string, runID string, wave int, ordinal int) string {
	name := sanitizeName(fmt.Sprintf("%s-%d-%d-%s", scenario, wave, ordinal, strings.ReplaceAll(runID, ".", "")))
	if len(name) <= 63 {
		return name
	}
	return name[:63]
}

func sanitizeName(value string) string {
	var out strings.Builder
	lastDash := false
	for _, r := range strings.ToLower(value) {
		allowed := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if allowed {
			out.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			out.WriteByte('-')
			lastDash = true
		}
	}
	name := strings.Trim(out.String(), "-")
	if name == "" {
		return "perf"
	}
	return name
}

func durationMillis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func writeReport(path string, report *runReport) error {
	payload, err := internaljson.Marshal(report)
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}
	if path == "" {
		return nil
	}
	if err = os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir report dir: %w", err)
	}
	if err = os.WriteFile(path, payload, 0o644); err != nil {
		return fmt.Errorf("write report: %w", err)
	}
	return nil
}

func writeRunOutput(format string, report *runReport) error {
	switch strings.ToLower(format) {
	case "json":
		payload, err := internaljson.Marshal(report)
		if err != nil {
			return fmt.Errorf("marshal report: %w", err)
		}
		_, err = fmt.Fprintln(os.Stdout, string(payload))
		return err
	case "table", "":
		return writeRunTable(os.Stdout, report)
	default:
		return fmt.Errorf("unsupported output format %q", format)
	}
}

func writeRunTable(w *os.File, report *runReport) error {
	if _, err := fmt.Fprintf(w, "runID=%s started=%s completed=%s\n\n", report.RunID, report.StartedAt.Format(time.RFC3339), report.CompletedAt.Format(time.RFC3339)); err != nil {
		return err
	}
	for _, scenario := range report.Scenarios {
		if scenario.Skipped {
			if _, err := fmt.Fprintf(w, "%s skipped: %s\n\n", scenario.Name, scenario.SkipReason); err != nil {
				return err
			}
			continue
		}
		if _, err := fmt.Fprintf(w, "%s\n", scenario.Name); err != nil {
			return err
		}
		if scenario.Description != "" {
			if _, err := fmt.Fprintf(w, "  %s\n", scenario.Description); err != nil {
				return err
			}
		}
		for _, wave := range scenario.Waves {
			if _, err := fmt.Fprintf(w, "  wave=%d size=%d cumulative=%d createFailures=%d success=%d failed=%t\n", wave.Wave, wave.Size, wave.CumulativeSize, wave.CreateFailures, wave.SuccessfulObjects, wave.WaveFailed); err != nil {
				return err
			}
			names := make([]string, 0, len(wave.HopSummaries))
			for name := range wave.HopSummaries {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, name := range names {
				summary := wave.HopSummaries[name]
				if _, err := fmt.Fprintf(w, "    %-20s count=%d ok=%d fail=%d min=%.2fms p50=%.2fms p95=%.2fms max=%.2fms mean=%.2fms\n", name, summary.Count, summary.Successes, summary.Failures, summary.MinMillis, summary.P50Millis, summary.P95Millis, summary.MaxMillis, summary.MeanMillis); err != nil {
					return err
				}
			}
		}
		if _, err := fmt.Fprintln(w); err != nil {
			return err
		}
	}
	return nil
}

func gvkFromManifestTemplate(manifest string) (string, string) {
	objects, err := decodeManifest(manifest)
	if err != nil || len(objects) == 0 {
		return "", ""
	}
	return objects[0].GetAPIVersion(), objects[0].GetKind()
}

func normalizedScope(scope string) string {
	switch strings.ToLower(strings.TrimSpace(scope)) {
	case "", scopeInstance:
		return scopeInstance
	case scopeWave:
		return scopeWave
	default:
		return strings.ToLower(strings.TrimSpace(scope))
	}
}

const (
	scopeInstance = "instance"
	scopeWave     = "wave"
)

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func percentile(values []float64, p float64) float64 {
	if len(values) == 0 {
		return 0
	}
	copied := append([]float64(nil), values...)
	sort.Float64s(copied)
	if len(copied) == 1 {
		return copied[0]
	}
	rank := p * float64(len(copied)-1)
	lower := int(math.Floor(rank))
	upper := int(math.Ceil(rank))
	if lower == upper {
		return copied[lower]
	}
	weight := rank - float64(lower)
	return copied[lower]*(1-weight) + copied[upper]*weight
}
