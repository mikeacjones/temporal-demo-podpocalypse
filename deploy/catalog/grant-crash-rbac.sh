#!/usr/bin/env bash
# Grants the booth the pod access its crash and k9s panel need, in one
# catalog runtime namespace. A stopgap until the platform can grant it: see
# README.md. Run it again after every rollout, since each version gets a new
# runtime namespace.
#
#   deploy/catalog/grant-crash-rbac.sh <kube-context> <runtime-namespace>
#
# The context is required so this never lands on whatever cluster kubectl
# happens to point at.
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <kube-context> <runtime-namespace>" >&2
  exit 2
fi
ctx=$1 ns=$2
k() { kubectl --context "$ctx" -n "$ns" "$@"; }

# Refuse a namespace without the booth's service account: wrong cluster or
# namespace, or the operator has not rolled the version out yet.
k get serviceaccount booth >/dev/null

# No platform.temporal.io/project label, so the operator neither owns nor
# prunes these. They go when the runtime namespace goes.
k apply -f - <<YAML
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: podpocalypse-booth-crash
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: podpocalypse-booth-crash
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: podpocalypse-booth-crash
subjects:
  - kind: ServiceAccount
    name: booth
    namespace: $ns
YAML

k auth can-i delete pods --as "system:serviceaccount:$ns:booth"
