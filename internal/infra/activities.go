package infra

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"k8s.io/client-go/kubernetes"

	"podpocalypse/internal/config"
	"podpocalypse/internal/names"
)

// Infra is the control-plane activity set. Kubernetes clients and the process
// supervisor live here, not in workflow inputs, so credentials never enter history.
type Infra struct {
	Runtime           string
	Namespace         string
	Image             string
	Client            client.Client
	Supervisor        *Supervisor
	Kube              kubernetes.Interface
	TemporalAddress   string
	TemporalNamespace string
	TemporalAPIKey    string
	TemporalTLSCert   string
	TemporalTLSKey    string
	TemporalTLSServer string
	LeadPath          string
	RecoveryDelay     time.Duration

	leadMu sync.Mutex
}

type ProvisionInput struct {
	SessionID string
}

type killMarker struct {
	UID string
	PID int
}

type Lead struct {
	SessionID  string    `json:"sessionId"`
	PlayerID   string    `json:"playerId"`
	Name       string    `json:"name"`
	Title      string    `json:"title"`
	Email      string    `json:"email"`
	Consent    bool      `json:"consent"`
	Score      int       `json:"score"`
	Crashes    int       `json:"crashes"`
	TimedOut   bool      `json:"timedOut"`
	FinishedAt time.Time `json:"finishedAt"`
}

type WorkerIdentity struct {
	Pod string `json:"pod"`
	PID int    `json:"pid"`
}

type SessionActivities struct{}

func (s *SessionActivities) IdentifyWorker(context.Context) (WorkerIdentity, error) {
	pod := os.Getenv("POD_NAME")
	if pod == "" {
		host, err := os.Hostname()
		if err != nil {
			return WorkerIdentity{}, err
		}
		pod = host
	}
	return WorkerIdentity{Pod: pod, PID: os.Getpid()}, nil
}

func (a *Infra) ProvisionSessionWorker(ctx context.Context, in ProvisionInput) error {
	if err := names.RequireSessionID(in.SessionID); err != nil {
		return temporal.NewNonRetryableApplicationError(err.Error(), "BadSession", err)
	}
	queue := names.SessionQueue(in.SessionID)
	switch a.Runtime {
	case config.RuntimeKubernetes:
		if err := a.ensureDeployment(ctx, in.SessionID, queue); err != nil {
			return err
		}
	case config.RuntimeProcess:
		if a.Supervisor == nil {
			return temporal.NewNonRetryableApplicationError("process supervisor is not configured", "BadRuntime", nil)
		}
		if err := a.Supervisor.Start(in.SessionID, queue); err != nil {
			return err
		}
	default:
		return temporal.NewNonRetryableApplicationError("unknown runtime "+a.Runtime, "BadRuntime", nil)
	}
	return a.waitForPoller(ctx, queue)
}

func (a *Infra) TeardownSessionWorker(ctx context.Context, sessionID string) error {
	switch a.Runtime {
	case config.RuntimeKubernetes:
		return a.deleteDeployment(ctx, sessionID)
	case config.RuntimeProcess:
		if a.Supervisor == nil {
			return nil
		}
		return a.Supervisor.Stop(sessionID)
	default:
		return temporal.NewNonRetryableApplicationError("unknown runtime "+a.Runtime, "BadRuntime", nil)
	}
}

// DeleteSessionPod is scheduled on the control task queue. It records the
// process or pod UID in a heartbeat before destroying it, then returns only
// after that generation is gone. A retry therefore cannot kill the replacement
// Kubernetes already started, and the session workflow cannot resume until the
// old worker has actually exited.
func (a *Infra) DeleteSessionPod(ctx context.Context, sessionID string) error {
	var marker killMarker
	if activity.IsActivity(ctx) && activity.HasHeartbeatDetails(ctx) {
		if err := activity.GetHeartbeatDetails(ctx, &marker); err != nil {
			return err
		}
	}
	if a.Runtime == config.RuntimeKubernetes {
		return a.killPod(ctx, sessionID, marker)
	}
	return a.killProcess(ctx, sessionID, marker)
}

func (a *Infra) killProcess(ctx context.Context, sessionID string, marker killMarker) error {
	if a.Supervisor == nil {
		return temporal.NewNonRetryableApplicationError("process supervisor is not configured", "BadRuntime", nil)
	}
	if marker.PID == 0 {
		marker.PID = a.Supervisor.CurrentPID(sessionID)
		if marker.PID == 0 {
			return temporal.NewApplicationError("session worker is not running yet", "WorkerNotRunning")
		}
		if activity.IsActivity(ctx) {
			activity.RecordHeartbeat(ctx, marker)
		}
	}
	return a.Supervisor.Kill(ctx, marker.PID)
}

func (a *Infra) killPod(ctx context.Context, sessionID string, marker killMarker) error {
	if marker.UID == "" {
		uid, err := a.currentUID(ctx, sessionID)
		if err != nil {
			return err
		}
		if uid == "" {
			return temporal.NewApplicationError("session pod is not running yet", "PodNotRunning")
		}
		marker.UID = uid
		if activity.IsActivity(ctx) {
			activity.RecordHeartbeat(ctx, marker)
		}
	}
	exists, err := a.uidExists(ctx, sessionID, marker.UID)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if err := a.deleteUID(ctx, sessionID, marker.UID); err != nil {
		return err
	}
	return a.waitUIDGone(ctx, sessionID, marker)
}

// holdRecovery keeps the session workflow parked after the old pod is gone.
// The replacement is often already polling by then; leaving the activity open
// is what makes the incident clock keep moving while the turn cannot.
func (a *Infra) holdRecovery(ctx context.Context, marker killMarker) error {
	if a.RecoveryDelay <= 0 {
		return nil
	}
	deadline := time.Now().Add(a.RecoveryDelay)
	tick := time.NewTicker(300 * time.Millisecond)
	defer tick.Stop()
	for time.Now().Before(deadline) {
		if activity.IsActivity(ctx) {
			activity.RecordHeartbeat(ctx, marker)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
	return nil
}

func (a *Infra) waitUIDGone(ctx context.Context, sessionID string, marker killMarker) error {
	tick := time.NewTicker(300 * time.Millisecond)
	defer tick.Stop()
	for {
		exists, err := a.uidExists(ctx, sessionID, marker.UID)
		if err != nil {
			return err
		}
		if !exists {
			return a.holdRecovery(ctx, marker)
		}
		if activity.IsActivity(ctx) {
			activity.RecordHeartbeat(ctx, marker)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func (a *Infra) waitForPoller(ctx context.Context, queue string) error {
	if a.Client == nil {
		return temporal.NewNonRetryableApplicationError("temporal client is not configured", "BadRuntime", nil)
	}
	tick := time.NewTicker(300 * time.Millisecond)
	defer tick.Stop()
	for {
		resp, err := a.Client.DescribeTaskQueue(ctx, queue, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
		if err != nil {
			return err
		}
		if len(resp.GetPollers()) > 0 {
			return nil
		}
		if activity.IsActivity(ctx) {
			activity.RecordHeartbeat(ctx, queue)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("no worker polling %s: %w", queue, ctx.Err())
		case <-tick.C:
		}
	}
}

func (a *Infra) AppendLead(_ context.Context, lead Lead) error {
	if a.LeadPath == "" {
		return nil
	}
	if lead.SessionID == "" || lead.Email == "" {
		return temporal.NewNonRetryableApplicationError("lead is missing session id or email", "BadLead", nil)
	}
	a.leadMu.Lock()
	defer a.leadMu.Unlock()

	existing, err := os.ReadFile(a.LeadPath)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	sc := bufio.NewScanner(strings.NewReader(string(existing)))
	for sc.Scan() {
		var row Lead
		if json.Unmarshal(sc.Bytes(), &row) == nil && row.SessionID == lead.SessionID {
			return nil
		}
	}
	f, err := os.OpenFile(a.LeadPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	enc, err := json.Marshal(lead)
	if err != nil {
		return err
	}
	enc = append(enc, '\n')
	_, err = f.Write(enc)
	return err
}
