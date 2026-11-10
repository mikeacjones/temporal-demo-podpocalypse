package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
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
	// RuntimePod runs every session on one long-lived worker pod that polls
	// SessionTaskQueue; a crash deletes that pod. The catalog runs this way,
	// since its components are fixed and the booth cannot create Deployments.
	RuntimePod = "pod"

	DefaultSessionQueue       = "podpocalypse-sessions"
	DefaultSessionPodSelector = "app.kubernetes.io/component=sessions"

	serviceAccountNamespaceFile = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
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
	// SessionQueue and SessionPodSelector are the shared session worker of
	// RuntimePod: the queue it polls and the label selector of its pod.
	SessionQueue       string
	SessionPodSelector string
	// K9s serves the read-only k9s panel when the booth can reach the
	// Kubernetes API. K9S=off turns it off.
	K9s           bool
	RecoveryDelay time.Duration
	GameDuration  time.Duration
	LeadPath      string
	AdminToken    string
	PodName       string
	AuthVerifyURL string
	PublicURL     string
	LeadsEndpoint string
	// DevAuthEmail stands in for sign-in on a local booth only.
	DevAuthEmail string
	// The field demo platform's sign-in (FIELDKIT_*, set on every demo pod).
	PlatformVerifyURL string
	PlatformCookie    string
	PlatformLoginURL  string
	// TrustIdentityHeaders takes X-Temporal-Auth-Email on a request as the
	// signed-in employee. Only safe behind the platform's edge, which strips
	// client-sent X-Temporal-Auth-* on every route; the catalog's does not.
	TrustIdentityHeaders bool
}

func FromEnv() (Config, error) {
	cfg := Config{
		Role:               env("ROLE", RoleBooth),
		HTTPAddr:           env("HTTP_ADDR", ":8080"),
		TemporalAddress:    env("TEMPORAL_ADDRESS", "127.0.0.1:7233"),
		TemporalNamespace:  env("TEMPORAL_NAMESPACE", "default"),
		TemporalAPIKey:     os.Getenv("TEMPORAL_API_KEY"),
		TemporalTLSCert:    os.Getenv("TEMPORAL_TLS_CERT"),
		TemporalTLSKey:     os.Getenv("TEMPORAL_TLS_KEY"),
		TemporalTLSServer:  os.Getenv("TEMPORAL_TLS_SERVER_NAME"),
		TemporalUI:         os.Getenv("TEMPORAL_UI_URL"),
		ControlQueue:       env("CONTROL_TASK_QUEUE", DefaultControlQueue),
		TaskQueue:          os.Getenv("TASK_QUEUE"),
		SessionID:          os.Getenv("SESSION_ID"),
		Runtime:            env("RUNTIME", RuntimeProcess),
		KubeNamespace:      env("POD_NAMESPACE", ownNamespace()),
		SessionImage:       env("SESSION_IMAGE", "podpocalypse:dev"),
		SessionQueue:       env("SESSION_TASK_QUEUE", DefaultSessionQueue),
		SessionPodSelector: env("SESSION_POD_SELECTOR", DefaultSessionPodSelector),
		K9s:                os.Getenv("K9S") != "off",
		RecoveryDelay:      4 * time.Second,
		GameDuration:       game.DefaultDuration,
		LeadPath:           os.Getenv("LEAD_PATH"),
		AdminToken:         os.Getenv("ADMIN_TOKEN"),
		PodName:            env("POD_NAME", ""),
		AuthVerifyURL:      os.Getenv("AUTH_VERIFY_URL"),
		PublicURL:          os.Getenv("PUBLIC_URL"),
		LeadsEndpoint:      os.Getenv("EVENT_LEADS_ENDPOINT"),
		DevAuthEmail:       os.Getenv("DEV_AUTH_EMAIL"),
		PlatformVerifyURL:  os.Getenv("FIELDKIT_AUTH_VERIFY_URL"),
		PlatformCookie:     os.Getenv("FIELDKIT_AUTH_COOKIE"),
		PlatformLoginURL:   os.Getenv("FIELDKIT_LOGIN_URL"),
	}
	// On by default exactly where the platform's edge sets the headers.
	cfg.TrustIdentityHeaders = cfg.PlatformVerifyURL != ""
	if v := os.Getenv("TRUST_PLATFORM_IDENTITY_HEADERS"); v != "" {
		trust, err := strconv.ParseBool(v)
		if err != nil {
			return Config{}, fmt.Errorf("TRUST_PLATFORM_IDENTITY_HEADERS: %w", err)
		}
		cfg.TrustIdentityHeaders = trust
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
	case RuntimeProcess, RuntimeKubernetes, RuntimePod:
	default:
		return Config{}, fmt.Errorf("RUNTIME must be process, kubernetes or pod, got %q", cfg.Runtime)
	}
	if cfg.DevAuthEmail != "" && (cfg.AuthVerifyURL != "" || cfg.PlatformVerifyURL != "" || !localURL(cfg.PublicURL)) {
		return Config{}, fmt.Errorf("DEV_AUTH_EMAIL needs a localhost PUBLIC_URL and no AUTH_VERIFY_URL or FIELDKIT_AUTH_VERIFY_URL")
	}
	if cfg.Role == RoleSession && cfg.TaskQueue == "" {
		return Config{}, fmt.Errorf("TASK_QUEUE is required for a session worker")
	}
	if cfg.Runtime == RuntimeKubernetes && cfg.Role == RoleBooth && cfg.SessionImage == "" {
		return Config{}, fmt.Errorf("SESSION_IMAGE is required when RUNTIME=kubernetes")
	}
	return cfg, nil
}

// UsesKube reports whether the booth talks to the Kubernetes API.
func (c Config) UsesKube() bool {
	return c.Runtime == RuntimeKubernetes || c.Runtime == RuntimePod
}

// ownNamespace is the pod's namespace from its service account token mount,
// for clusters where the manifest cannot set POD_NAMESPACE from the downward
// API (the catalog's DemoProject has no fieldRef).
func ownNamespace() string {
	ns, err := os.ReadFile(serviceAccountNamespaceFile)
	if err != nil || strings.TrimSpace(string(ns)) == "" {
		return "podpocalypse"
	}
	return strings.TrimSpace(string(ns))
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func localURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	host := u.Hostname()
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
