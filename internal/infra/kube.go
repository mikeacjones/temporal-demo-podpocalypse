package infra

import (
	"context"
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"podpocalypse/internal/names"
)

const (
	appLabelKey   = "app"
	appLabelValue = "podpocalypse-session"
	sessionLabel  = "podpocalypse.io/session"
	createdLabel  = "podpocalypse.io/created"
)

func (a *Infra) ensureDeployment(ctx context.Context, sessionID, taskQueue string) error {
	if a.Kube == nil {
		return fmt.Errorf("kubernetes client is not configured")
	}
	name := names.DeploymentName(sessionID)
	_, err := a.Kube.AppsV1().Deployments(a.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	_, err = a.Kube.AppsV1().Deployments(a.Namespace).Create(ctx, a.deployment(sessionID, taskQueue), metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (a *Infra) deployment(sessionID, taskQueue string) *appsv1.Deployment {
	labels := map[string]string{
		appLabelKey:  appLabelValue,
		sessionLabel: sessionID,
		createdLabel: strconv.FormatInt(time.Now().Unix(), 10),
	}
	replicas := int32(1)
	grace := int64(2)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      names.DeploymentName(sessionID),
			Namespace: a.Namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{
				appLabelKey:  appLabelValue,
				sessionLabel: sessionID,
			}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
					appLabelKey:  appLabelValue,
					sessionLabel: sessionID,
				}},
				Spec: corev1.PodSpec{
					TerminationGracePeriodSeconds: &grace,
					Containers: []corev1.Container{{
						Name:            "worker",
						Image:           a.Image,
						ImagePullPolicy: corev1.PullIfNotPresent,
						Args:            []string{"-role", "session"},
						Env:             a.sessionEnv(sessionID, taskQueue),
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("20m"),
								corev1.ResourceMemory: resource.MustParse("64Mi"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("256Mi"),
							},
						},
					}},
				},
			},
		},
	}
}

func (a *Infra) sessionEnv(sessionID, taskQueue string) []corev1.EnvVar {
	env := []corev1.EnvVar{
		{Name: "ROLE", Value: "session"},
		{Name: "TASK_QUEUE", Value: taskQueue},
		{Name: "SESSION_ID", Value: sessionID},
		{Name: "TEMPORAL_ADDRESS", Value: a.TemporalAddress},
		{Name: "TEMPORAL_NAMESPACE", Value: a.TemporalNamespace},
		{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
	}
	if a.TemporalAPIKey != "" {
		env = append(env, corev1.EnvVar{Name: "TEMPORAL_API_KEY", Value: a.TemporalAPIKey})
	}
	if a.TemporalTLSCert != "" {
		env = append(env, corev1.EnvVar{Name: "TEMPORAL_TLS_CERT", Value: a.TemporalTLSCert})
	}
	if a.TemporalTLSKey != "" {
		env = append(env, corev1.EnvVar{Name: "TEMPORAL_TLS_KEY", Value: a.TemporalTLSKey})
	}
	if a.TemporalTLSServer != "" {
		env = append(env, corev1.EnvVar{Name: "TEMPORAL_TLS_SERVER_NAME", Value: a.TemporalTLSServer})
	}
	return env
}

func (a *Infra) currentUID(ctx context.Context, sessionID string) (string, error) {
	pods, err := a.listSessionPods(ctx, sessionID)
	if err != nil {
		return "", err
	}
	var chosen *corev1.Pod
	for i := range pods {
		pod := &pods[i]
		if pod.DeletionTimestamp != nil {
			continue
		}
		if chosen == nil || pod.Status.Phase == corev1.PodRunning {
			chosen = pod
			if pod.Status.Phase == corev1.PodRunning {
				break
			}
		}
	}
	if chosen == nil {
		return "", nil
	}
	return string(chosen.UID), nil
}

func (a *Infra) listSessionPods(ctx context.Context, sessionID string) ([]corev1.Pod, error) {
	list, err := a.Kube.CoreV1().Pods(a.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: sessionLabel + "=" + sessionID,
	})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

// deleteUID deletes one pod generation. A Deployment may already have created
// the replacement; that pod has a different UID and must keep running.
func (a *Infra) deleteUID(ctx context.Context, sessionID, uid string) error {
	pods, err := a.listSessionPods(ctx, sessionID)
	if err != nil {
		return err
	}
	for _, pod := range pods {
		if string(pod.UID) != uid {
			continue
		}
		target := types.UID(uid)
		grace := int64(1)
		err := a.Kube.CoreV1().Pods(a.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{
			GracePeriodSeconds: &grace,
			Preconditions:      &metav1.Preconditions{UID: &target},
		})
		if apierrors.IsNotFound(err) {
			return nil
		}
		return err
	}
	return nil
}

func (a *Infra) uidExists(ctx context.Context, sessionID, uid string) (bool, error) {
	pods, err := a.listSessionPods(ctx, sessionID)
	if err != nil {
		return false, err
	}
	for _, pod := range pods {
		if string(pod.UID) == uid {
			return true, nil
		}
	}
	return false, nil
}

func (a *Infra) deleteDeployment(ctx context.Context, sessionID string) error {
	if a.Kube == nil {
		return nil
	}
	err := a.Kube.AppsV1().Deployments(a.Namespace).Delete(ctx, names.DeploymentName(sessionID), metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func (a *Infra) ReapExpiredSessions(ctx context.Context, olderThan time.Duration) error {
	if a.Kube == nil {
		return nil
	}
	list, err := a.Kube.AppsV1().Deployments(a.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: appLabelKey + "=" + appLabelValue,
	})
	if err != nil {
		return err
	}
	cutoff := time.Now().Add(-olderThan).Unix()
	for _, dep := range list.Items {
		created, err := strconv.ParseInt(dep.Labels[createdLabel], 10, 64)
		if err != nil || created > cutoff {
			continue
		}
		if err := a.Kube.AppsV1().Deployments(a.Namespace).Delete(ctx, dep.Name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
