// Package k9s streams a real, read-only k9s of the booth's namespace to the
// game screen, so players watch Kubernetes delete the session pod and start
// its replacement.
package k9s

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/creack/pty"
	"k8s.io/client-go/rest"
)

//go:embed config
var configFiles embed.FS

const serviceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// maxSessions bounds how many k9s processes viewers can start at once. Each
// open game screen holds one while a flight is on.
const maxSessions = 3

// Terminal runs k9s in a PTY per viewer and streams it over a WebSocket.
// Binary frames carry terminal output to the browser. The browser only sends
// text frames with its size ({"type":"resize"}); keystrokes are never
// forwarded, so the panel cannot drive k9s.
type Terminal struct {
	command []string
	env     []string
	active  atomic.Int32
}

// InCluster prepares k9s to watch namespace as the pod's own service account,
// never a developer kubeconfig. It fails when the booth is not running in a
// cluster with a mounted token, or k9s is not installed.
func InCluster(namespace, workDir string) (*Terminal, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	bin, err := exec.LookPath("k9s")
	if err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp(workDir, "k9s-")
	if err != nil {
		return nil, err
	}
	cfgDir := filepath.Join(dir, "config")
	if err := writeConfig(cfgDir); err != nil {
		return nil, fmt.Errorf("write k9s config: %w", err)
	}
	// tokenFile, not an inlined token, so k9s picks up projected-token rotation.
	kubeconfig := filepath.Join(dir, "kubeconfig")
	kc := fmt.Sprintf(`apiVersion: v1
kind: Config
clusters:
- name: booth
  cluster:
    server: %s
    certificate-authority: %s/ca.crt
users:
- name: booth
  user:
    tokenFile: %s/token
contexts:
- name: booth
  context:
    cluster: booth
    user: booth
    namespace: %s
current-context: booth
`, cfg.Host, serviceAccountDir, serviceAccountDir, namespace)
	if err := os.WriteFile(kubeconfig, []byte(kc), 0o600); err != nil {
		return nil, fmt.Errorf("write kubeconfig: %w", err)
	}
	return &Terminal{
		command: []string{bin, "--context", "booth", "-n", namespace, "-c", "pods", "--readonly",
			"--headless", "--logoless", "--crumbsless", "--splashless", "--refresh", "1"},
		env: []string{
			"K9S_CONFIG_DIR=" + cfgDir,
			"KUBECONFIG=" + kubeconfig,
			"HOME=" + dir,
			"XDG_STATE_HOME=" + dir,
			"XDG_CACHE_HOME=" + dir,
			"XDG_DATA_HOME=" + dir,
			"TERM=xterm-256color",
			"COLORTERM=truecolor",
		},
	}, nil
}

func writeConfig(dst string) error {
	return fs.WalkDir(configFiles, "config", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel("config", path)
		if err != nil {
			return err
		}
		out := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(out, 0o755)
		}
		data, err := configFiles.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(out, data, 0o644)
	})
}

type control struct {
	Type string `json:"type"`
	Cols uint16 `json:"cols"`
	Rows uint16 `json:"rows"`
}

func (t *Terminal) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if t.active.Add(1) > maxSessions {
		t.active.Add(-1)
		http.Error(w, "too many k9s viewers", http.StatusTooManyRequests)
		return
	}
	defer t.active.Add(-1)

	// Accept rejects cross-origin requests by default, so another site open
	// in the booth browser cannot attach.
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer conn.CloseNow()
	conn.SetReadLimit(4 << 10)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	cmd := exec.Command(t.command[0], t.command[1:]...)
	cmd.Env = append(os.Environ(), t.env...)
	ptmx, err := pty.StartWithSize(cmd, &pty.Winsize{Cols: 80, Rows: 8})
	if err != nil {
		slog.Warn("start k9s", "err", err)
		_ = conn.Close(websocket.StatusInternalError, "k9s did not start")
		return
	}
	defer stop(cmd, ptmx)

	go func() {
		defer cancel()
		buf := make([]byte, 32<<10)
		for {
			n, err := ptmx.Read(buf)
			if n > 0 {
				if werr := conn.Write(ctx, websocket.MessageBinary, buf[:n]); werr != nil {
					return
				}
			}
			if err != nil {
				_ = conn.Close(websocket.StatusNormalClosure, "k9s exited")
				return
			}
		}
	}()

	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			continue
		}
		var c control
		if json.Unmarshal(data, &c) == nil && c.Type == "resize" && c.Cols > 0 && c.Rows > 0 && c.Cols <= 400 && c.Rows <= 100 {
			_ = pty.Setsize(ptmx, &pty.Winsize{Cols: c.Cols, Rows: c.Rows})
		}
	}
}

// stop ends k9s with SIGTERM, then SIGKILL if it has not exited in 2s.
func stop(cmd *exec.Cmd, ptmx *os.File) {
	_ = ptmx.Close()
	if cmd.Process == nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		_ = cmd.Process.Kill()
		<-done
	}
}
