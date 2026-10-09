package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"podpocalypse/internal/game"
)

const (
	DefaultControlQueue = "podpocalypse-control"
	RoleBooth           = "booth"
	RoleSession         = "session"
	RuntimeProcess      = "process"
	RuntimeKubernetes   = "kubernetes"
)

type Config struct {
	Role              string
	HTTPAddr          string
	TemporalAddress   string
	TemporalNamespace string
	TemporalAPIKey    string
	TemporalTLSCert   string
	TemporalTLSKey    string
	TemporalTLSServer string
	TemporalUI        string
	ControlQueue      string
	TaskQueue         string
	SessionID         string
	Runtime           string
	KubeNamespace     string
	SessionImage      string
	RecoveryDelay     time.Duration
	GameDuration      time.Duration
	LeadPath          string
	AdminToken        string
	PodName           string
	AuthVerifyURL     string
	PublicURL         string
	LeadsEndpoint     string
	LeadsURL          string
}

func FromEnv() (Config, error) {
	cfg := Config{
		Role:              env("ROLE", RoleBooth),
		HTTPAddr:          env("HTTP_ADDR", ":8080"),
		TemporalAddress:   env("TEMPORAL_ADDRESS", "127.0.0.1:7233"),
		TemporalNamespace: env("TEMPORAL_NAMESPACE", "default"),
		TemporalAPIKey:    os.Getenv("TEMPORAL_API_KEY"),
		TemporalTLSCert:   os.Getenv("TEMPORAL_TLS_CERT"),
		TemporalTLSKey:    os.Getenv("TEMPORAL_TLS_KEY"),
		TemporalTLSServer: os.Getenv("TEMPORAL_TLS_SERVER_NAME"),
		TemporalUI:        os.Getenv("TEMPORAL_UI_URL"),
		ControlQueue:      env("CONTROL_TASK_QUEUE", DefaultControlQueue),
		TaskQueue:         os.Getenv("TASK_QUEUE"),
		SessionID:         os.Getenv("SESSION_ID"),
		Runtime:           env("RUNTIME", RuntimeProcess),
		KubeNamespace:     env("POD_NAMESPACE", "podpocalypse"),
		SessionImage:      env("SESSION_IMAGE", "podpocalypse:dev"),
		RecoveryDelay:     4 * time.Second,
		GameDuration:      game.DefaultDuration,
		LeadPath:          os.Getenv("LEAD_PATH"),
		AdminToken:        os.Getenv("ADMIN_TOKEN"),
		PodName:           env("POD_NAME", ""),
		AuthVerifyURL:     os.Getenv("AUTH_VERIFY_URL"),
		PublicURL:         os.Getenv("PUBLIC_URL"),
		LeadsEndpoint:     os.Getenv("EVENT_LEADS_ENDPOINT"),
		LeadsURL:          strings.TrimRight(os.Getenv("EVENT_LEADS_URL"), "/"),
	}
	if v := os.Getenv("RECOVERY_DELAY"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, fmt.Errorf("RECOVERY_DELAY: %w", err)
		}
		cfg.RecoveryDelay = d
	}
	if v := os.Getenv("GAME_DURATION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return Config{}, fmt.Errorf("GAME_DURATION: %w", err)
		}
		cfg.GameDuration = d
	}
	switch cfg.Role {
	case RoleBooth, RoleSession:
	default:
		return Config{}, fmt.Errorf("ROLE must be booth or session, got %q", cfg.Role)
	}
	switch cfg.Runtime {
	case RuntimeProcess, RuntimeKubernetes:
	default:
		return Config{}, fmt.Errorf("RUNTIME must be process or kubernetes, got %q", cfg.Runtime)
	}
	if cfg.Role == RoleSession && cfg.TaskQueue == "" {
		return Config{}, fmt.Errorf("TASK_QUEUE is required for a session worker")
	}
	if cfg.Runtime == RuntimeKubernetes && cfg.Role == RoleBooth && cfg.SessionImage == "" {
		return Config{}, fmt.Errorf("SESSION_IMAGE is required when RUNTIME=kubernetes")
	}
	return cfg, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
