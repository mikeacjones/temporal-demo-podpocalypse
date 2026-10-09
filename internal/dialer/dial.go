package dialer

import (
	"crypto/tls"
	"fmt"

	"go.temporal.io/sdk/client"

	"podpocalypse/internal/config"
)

func Dial(cfg config.Config) (client.Client, error) {
	opts := client.Options{
		HostPort:  cfg.TemporalAddress,
		Namespace: cfg.TemporalNamespace,
	}
	var tlsCfg *tls.Config
	if cfg.TemporalTLSCert != "" || cfg.TemporalTLSKey != "" {
		if cfg.TemporalTLSCert == "" || cfg.TemporalTLSKey == "" {
			return nil, fmt.Errorf("TEMPORAL_TLS_CERT and TEMPORAL_TLS_KEY must both be set")
		}
		cert, err := tls.LoadX509KeyPair(cfg.TemporalTLSCert, cfg.TemporalTLSKey)
		if err != nil {
			return nil, fmt.Errorf("load temporal mTLS certificate: %w", err)
		}
		tlsCfg = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	}
	if cfg.TemporalAPIKey != "" {
		opts.Credentials = client.NewAPIKeyStaticCredentials(cfg.TemporalAPIKey)
		if tlsCfg == nil {
			tlsCfg = &tls.Config{MinVersion: tls.VersionTLS12}
		}
	}
	if tlsCfg != nil {
		if cfg.TemporalTLSServer != "" {
			tlsCfg.ServerName = cfg.TemporalTLSServer
		}
		opts.ConnectionOptions.TLS = tlsCfg
	}
	return client.Dial(opts)
}
