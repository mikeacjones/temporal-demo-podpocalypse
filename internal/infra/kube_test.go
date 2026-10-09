package infra

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"

	"podpocalypse/internal/config"
)

func TestDeleteUIDSparesTheReplacementPod(t *testing.T) {
	oldPod := sessionPod("abcd1234", "sess-old", "uid-old")
	newPod := sessionPod("abcd1234", "sess-new", "uid-new")
	kube := fake.NewClientset(oldPod, newPod)
	a := &Infra{Kube: kube, Namespace: "podpocalypse", Runtime: config.RuntimeKubernetes}

	if err := a.deleteUID(context.Background(), "abcd1234", "uid-old"); err != nil {
		t.Fatal(err)
	}
	pods, err := a.listSessionPods(context.Background(), "abcd1234")
	if err != nil {
		t.Fatal(err)
	}
	if len(pods) != 1 || string(pods[0].UID) != "uid-new" {
		t.Fatalf("pods after targeted delete: %+v", pods)
	}
	exists, err := a.uidExists(context.Background(), "abcd1234", "uid-old")
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("old uid still reported alive")
	}
}

func TestTerminatingPodStillBlocksResume(t *testing.T) {
	pod := sessionPod("abcd1234", "sess-old", "uid-old")
	now := metav1.Now()
	pod.DeletionTimestamp = &now
	a := &Infra{Kube: fake.NewClientset(pod), Namespace: "podpocalypse", Runtime: config.RuntimeKubernetes}
	exists, err := a.uidExists(context.Background(), "abcd1234", "uid-old")
	if err != nil {
		t.Fatal(err)
	}
	if !exists {
		t.Fatal("a terminating pod was treated as already gone")
	}
}

func TestReapExpiredSessionsLeavesFreshDeployments(t *testing.T) {
	kube := fake.NewClientset()
	a := &Infra{Kube: kube, Namespace: "podpocalypse", Runtime: config.RuntimeKubernetes, Image: "podpocalypse:dev"}
	ctx := context.Background()
	if err := a.ensureDeployment(ctx, "abcd1234", "session-abcd1234"); err != nil {
		t.Fatal(err)
	}
	stale := a.deployment("bbbbbbbb", "session-bbbbbbbb")
	stale.Labels[createdLabel] = "1"
	if _, err := kube.AppsV1().Deployments("podpocalypse").Create(ctx, stale, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := a.ReapExpiredSessions(ctx, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := kube.AppsV1().Deployments("podpocalypse").Get(ctx, "sess-abcd1234", metav1.GetOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := kube.AppsV1().Deployments("podpocalypse").Get(ctx, "sess-bbbbbbbb", metav1.GetOptions{}); err == nil {
		t.Fatal("stale deployment was not reaped")
	}
}

func sessionPod(sessionID, name, uid string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "podpocalypse",
			Labels: map[string]string{
				appLabelKey:  appLabelValue,
				sessionLabel: sessionID,
			},
			UID: types.UID(uid),
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}
