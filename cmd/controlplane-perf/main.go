// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/alecthomas/kong"
)

type cli struct {
	Inventory cmdInventory `cmd:"" help:"List watched CRDs/controllers and the downstream resources they touch."`
	Run       cmdRun       `cmd:"" help:"Run a manifest-driven control-plane perf plan against a Kubernetes cluster."`
}

type cmdInventory struct {
	Format string `name:"format" enum:"table,json" default:"table" help:"Output format."`
}

type cmdRun struct {
	Plan               string   `arg:"" type:"path" optional:"" help:"Path to the perf plan YAML file."`
	Report             string   `name:"report" type:"path" help:"Optional JSON report output path."`
	Output             string   `name:"output" enum:"table,json" default:"table" help:"Stdout output format."`
	PrintExampleConfig bool     `name:"print-example-config" help:"Print an example plan YAML and exit."`
	Set                []string `name:"set" help:"Template variable override in key=value form."`
	Kubeconfig         string   `name:"kubeconfig" env:"KUBECONFIG" type:"path" help:"Path to kubeconfig. Falls back to in-cluster config when unset."`
	Context            string   `name:"context" help:"Kubeconfig context override."`
	QPS                float32  `name:"qps" default:"50" help:"REST client QPS."`
	Burst              int      `name:"burst" default:"100" help:"REST client burst."`
}

func main() {
	ctx := context.Background()
	var app cli
	parser, err := kong.New(&app,
		kong.Name("controlplane-perf"),
		kong.Description("Envoy AI Gateway control-plane performance harness"),
	)
	if err != nil {
		log.Fatalf("create parser: %v", err)
	}
	parsed, err := parser.Parse(os.Args[1:])
	parser.FatalIfErrorf(err)
	switch parsed.Command() {
	case "inventory":
		if err = writeInventory(os.Stdout, app.Inventory.Format); err != nil {
			log.Fatalf("inventory: %v", err)
		}
	case "run", "run <plan>":
		if app.Run.PrintExampleConfig {
			_, _ = fmt.Fprint(os.Stdout, samplePlan)
			return
		}
		if app.Run.Plan == "" {
			log.Fatal("run requires a plan path unless --print-example-config is set")
		}
		plan, loadErr := loadPlan(app.Run.Plan)
		if loadErr != nil {
			log.Fatalf("load plan: %v", loadErr)
		}
		cluster, clusterErr := newClusterClients(ctx, kubeFlags{
			Kubeconfig: app.Run.Kubeconfig,
			Context:    app.Run.Context,
			QPS:        app.Run.QPS,
			Burst:      app.Run.Burst,
		})
		if clusterErr != nil {
			log.Fatalf("cluster: %v", clusterErr)
		}
		telemetry, telemetryErr := newHarnessTelemetry(ctx)
		if telemetryErr != nil {
			log.Fatalf("telemetry: %v", telemetryErr)
		}
		defer func() {
			if shutdownErr := telemetry.Shutdown(context.Background()); shutdownErr != nil {
				log.Printf("telemetry shutdown: %v", shutdownErr)
			}
		}()
		report, runErr := executePlan(ctx, cluster, plan, app.Run.Set, telemetry)
		if runErr != nil {
			log.Fatalf("run: %v", runErr)
		}
		if app.Run.Report != "" {
			if writeErr := writeReport(app.Run.Report, report); writeErr != nil {
				log.Fatalf("write report: %v", writeErr)
			}
		}
		if runErr = writeRunOutput(app.Run.Output, report); runErr != nil {
			log.Fatalf("output: %v", runErr)
		}
	default:
		log.Fatalf("unsupported command %q", parsed.Command())
	}
}
