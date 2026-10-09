// Package names holds workflow IDs and task queue names shared by the API,
// workflows, and the worker provisioner. Session IDs are also Kubernetes labels.
package names

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
)

const (
	LeaderboardWorkflowID = "leaderboard"
	KioskWorkflowID       = "kiosk"
)

var sessionIDPattern = regexp.MustCompile(`^[a-f0-9]{8}$`)

// NewSessionID returns an 8-character hex ID safe to embed in a Deployment name.
func NewSessionID() (string, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

func ValidSessionID(id string) bool {
	return sessionIDPattern.MatchString(id)
}

func SessionQueue(sessionID string) string {
	return "session-" + sessionID
}

func SessionWorkflowID(sessionID string) string {
	return "session-" + sessionID
}

func RunWorkflowID(sessionID string) string {
	return "run-" + sessionID
}

func DeploymentName(sessionID string) string {
	return "sess-" + sessionID
}

// PlayerWorkflowID hashes the email so workflow IDs and URLs are not the address itself.
func PlayerWorkflowID(email string) string {
	sum := sha256.Sum256([]byte(normalizeEmail(email)))
	return "player-" + hex.EncodeToString(sum[:8])
}

func normalizeEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}

func RequireSessionID(id string) error {
	if !ValidSessionID(id) {
		return fmt.Errorf("session id %q is not 8 lowercase hex characters", id)
	}
	return nil
}
