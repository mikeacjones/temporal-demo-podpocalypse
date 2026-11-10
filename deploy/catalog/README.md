# A real pod crash in the catalog

In the catalog, a wreck deletes a real Kubernetes pod: the shared session
worker pod, which its Deployment replaces. The game uses `RUNTIME=pod` for
this (see the [README](../../README.md#how-the-workflows-are-cut)).

The platform cannot yet grant a component Kubernetes RBAC. Until it can,
someone grants the booth access to pods by hand, once per rollout. Everything
else goes through the `DemoProject`.

## 1. Change the DemoProject

In `temporal-sa/temporal-fieldkit-platform`, edit
`projects/live/demo/podpocalypse.yaml` (and `projects/test/demo/podpocalypse.yaml`).
Replace the `booth` component's `RUNTIME` value, mount its token, and add the
`sessions` component:

```yaml
  components:
    booth:
      build: app
      worker: true
      temporalAccess: true
      # The booth deletes the session pod and runs the k9s panel as this
      # account. Its pod RBAC is granted by hand (grant-crash-rbac.sh).
      automountServiceAccountToken: true
      env:
        - name: RUNTIME
          value: pod
        # PUBLIC_URL, CONTROL_TASK_QUEUE, EVENT_LEADS_ENDPOINT as before
      ports:
        - name: http
          containerPort: 8080
          servicePort: 80
    # Every flight's session worker. A crash deletes this pod.
    sessions:
      build: app
      worker: true
      # Fixed at one: autoscaling would add pods the crash does not delete.
      replicas: 1
      env:
        - name: ROLE
          value: session
        - name: TASK_QUEUE
          value: podpocalypse-sessions
```

The sessions pod's labels are the operator's
(`app.kubernetes.io/component: sessions`), which is the booth's default
`SESSION_POD_SELECTOR`. If you name the component something else, set
`SESSION_POD_SELECTOR` on the booth to match.

## 2. Grant the booth pod access after each rollout

Each version runs in its own runtime namespace,
`fieldkit-podpocalypse-<hash>`, and a hand-made Role goes with that namespace
when the next version replaces it. Grant it as soon as the new namespace
exists. Until then, a crash fails after its retries and the k9s panel shows
k9s's "forbidden" error.

Find the namespace the booth runs in now:

```bash
kubectl --context <live-context> -n fieldkit-podpocalypse get svc booth \
  -o jsonpath='{.metadata.annotations.platform\.temporal\.io/active-namespace}'
```

Or list the project's runtime namespaces (the newest is the candidate):

```bash
kubectl --context <live-context> get ns \
  -l platform.temporal.io/project=podpocalypse,platform.temporal.io/runtime-namespace=true
```

Then grant it:

```bash
deploy/catalog/grant-crash-rbac.sh <live-context> fieldkit-podpocalypse-<hash>
```

The script checks the booth's ServiceAccount exists in that namespace, applies
a Role and RoleBinding (`get`, `list`, `watch`, `delete` on pods), and prints
`yes` when the booth can delete pods. The objects carry no
`platform.temporal.io/project` label, so the operator does not own or prune
them.

## 3. Check it

Play a flight at https://podpocalypse.tmprl-demo.cloud and crash. The k9s
strip shows the `sessions-…` pod terminate and a new one start, and the Event
History strip says the new pod replayed the flight. From a terminal:

```bash
kubectl --context <live-context> -n fieldkit-podpocalypse-<hash> get pods -w
```

## Undo

Set the booth's `RUNTIME` back to `process`, drop
`automountServiceAccountToken` and the `sessions` component. The next rollout
leaves the Role behind in the old namespace, which is deleted with it.

## Try it locally

[`kind-runtime-namespace.yaml`](kind-runtime-namespace.yaml) lays out one
runtime namespace the way the operator does, with a dev server in place of
Temporal Cloud:

```bash
kind create cluster --name podpocalypse
docker build -t podpocalypse:dev . && docker pull temporalio/temporal:1.5.1
kind load docker-image podpocalypse:dev temporalio/temporal:1.5.1 --name podpocalypse
kubectl --context kind-podpocalypse apply -f deploy/catalog/kind-runtime-namespace.yaml
deploy/catalog/grant-crash-rbac.sh kind-podpocalypse fieldkit-podpocalypse-abcde
kubectl --context kind-podpocalypse -n fieldkit-podpocalypse-abcde port-forward svc/booth 18090:80
```

Open http://localhost:18090 and fly. `kind create cluster` switches your
current kubectl context to the new cluster, so switch it back afterwards.
