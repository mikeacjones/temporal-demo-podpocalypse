package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"podpocalypse/internal/api"
	"podpocalypse/internal/config"
	"podpocalypse/internal/dialer"
	"podpocalypse/internal/infra"
	"podpocalypse/internal/names"
	"podpocalypse/internal/workflows"
)

func main() {
	roleFlag := flag.String("role", "", "booth or session")
	flag.Parse()
	cfg, err := config.FromEnv()
	if err != nil {
		slog.Error("config", "err", err)
		os.Exit(1)
	}
	if *roleFlag != "" {
		cfg.Role = *roleFlag
	}
	if cfg.Role == config.RoleSession && cfg.TaskQueue == "" {
		slog.Error("TASK_QUEUE is required for a session worker")
		os.Exit(1)
	}

	c, err := dialer.Dial(cfg)
	if err != nil {
		slog.Error("temporal dial", "err", err)
		os.Exit(1)
	}
	defer c.Close()

	if cfg.Role == config.RoleSession {
		if err := runSession(c, cfg); err != nil {
			slog.Error("session worker", "err", err)
			os.Exit(1)
		}
		return
	}
	if err := runBooth(c, cfg); err != nil {
		slog.Error("booth", "err", err)
		os.Exit(1)
	}
}

func runSession(c client.Client, cfg config.Config) error {
	opts := worker.Options{}
	if cfg.PodName != "" {
		opts.Identity = cfg.PodName
	}
	w := worker.New(c, cfg.TaskQueue, opts)
	w.RegisterWorkflow(workflows.GameSessionWorkflow)
	w.RegisterActivity(&infra.SessionActivities{})
	slog.Info("session worker polling", "taskQueue", cfg.TaskQueue, "pod", cfg.PodName)
	return w.Run(worker.InterruptCh())
}

func runBooth(c client.Client, cfg config.Config) error {
	acts := &infra.Infra{
		Runtime:           cfg.Runtime,
		Namespace:         cfg.KubeNamespace,
		Image:             cfg.SessionImage,
		Client:            c,
		TemporalAddress:   cfg.TemporalAddress,
		TemporalNamespace: cfg.TemporalNamespace,
		TemporalAPIKey:    cfg.TemporalAPIKey,
		TemporalTLSCert:   cfg.TemporalTLSCert,
		TemporalTLSKey:    cfg.TemporalTLSKey,
		TemporalTLSServer: cfg.TemporalTLSServer,
		LeadPath:          cfg.LeadPath,
		RecoveryDelay:     cfg.RecoveryDelay,
	}
	if cfg.Runtime == config.RuntimeProcess {
		acts.Supervisor = &infra.Supervisor{Delay: cfg.RecoveryDelay, BaseEnv: os.Environ()}
	} else {
		kube, err := kubeClient()
		if err != nil {
			return err
		}
		acts.Kube = kube
	}

	w := worker.New(c, cfg.ControlQueue, worker.Options{})
	w.RegisterWorkflow(workflows.GameRunWorkflow)
	w.RegisterWorkflow(workflows.LeaderboardWorkflow)
	w.RegisterWorkflow(workflows.PlayerWorkflow)
	w.RegisterActivity(acts)

	interrupt := worker.InterruptCh()
	go func() {
		if err := w.Run(interrupt); err != nil {
			slog.Error("control worker stopped", "err", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := waitForPoller(ctx, c, cfg.ControlQueue); err != nil {
		return fmt.Errorf("control worker did not poll: %w", err)
	}
	if err := ensureLeaderboard(ctx, c, cfg.ControlQueue); err != nil {
		return err
	}

	if cfg.Runtime == config.RuntimeKubernetes {
		go reapLoop(interrupt, acts)
	}

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           (&api.Server{Client: c, Config: cfg}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-interrupt
		if acts.Supervisor != nil {
			acts.Supervisor.StopAll()
		}
		shutdownCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		_ = srv.Shutdown(shutdownCtx)
	}()

	slog.Info("booth listening", "addr", cfg.HTTPAddr, "runtime", cfg.Runtime, "temporal", cfg.TemporalAddress)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func reapLoop(interrupt <-chan interface{}, acts *infra.Infra) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-interrupt:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			if err := acts.ReapExpiredSessions(ctx, 20*time.Minute); err != nil {
				slog.Error("reap session workers", "err", err)
			}
			cancel()
		}
	}
}

func ensureLeaderboard(ctx context.Context, c client.Client, queue string) error {
	_, err := c.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                    names.LeaderboardWorkflowID,
		TaskQueue:             queue,
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
	}, workflows.LeaderboardWorkflow, workflows.Board{})
	if temporal.IsWorkflowExecutionAlreadyStartedError(err) {
		return nil
	}
	return err
}

func waitForPoller(ctx context.Context, c client.Client, queue string) error {
	for {
		resp, err := c.DescribeTaskQueue(ctx, queue, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
		if err == nil && len(resp.GetPollers()) > 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			if err != nil {
				return err
			}
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func kubeClient() (kubernetes.Interface, error) {
	kcfg, err := rest.InClusterConfig()
	if err != nil {
		kcfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			clientcmd.NewDefaultClientConfigLoadingRules(),
			&clientcmd.ConfigOverrides{},
		).ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("kubernetes config: %w", err)
		}
	}
	return kubernetes.NewForConfig(kcfg)
}
