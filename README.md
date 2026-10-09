# Kill the worker

A KubeCon booth game. Players leave a name, title, and email, then pilot Ziggy
in a Kubernetes pod-ship through the cluster for 60 seconds. Tap to climb. Hit
a wall and the ship breaks apart while Kubernetes deletes the worker running
that flight. The clock keeps moving, and you cannot score until the scheduler
brings up a replacement pod and Temporal replays the workflow. That lost time
is the penalty.

After the opening gates, slow `OOM` memory blocks enter the flight path. One
late-game rolling-restart wave follows a visible warning and leaves a generous
gap. Both hazards trigger the same real worker deletion without turning the
booth game into a bullet storm.

The full-color Ziggy artwork comes from the official
[`temporalio/ui`](https://github.com/temporalio/ui) repository.
The flight music is [Outer Space Loop](https://opengameart.org/content/outer-space-loop)
by wipics, released CC0.

## How the workflows are cut

The booth process is the worker you do not kill. It polls `podpocalypse-control`
and runs three workflows:

- `GameRunWorkflow` (`run-<session>`) creates the session worker, starts the
  game as a child, then records the score and tears the worker down.
- `PlayerWorkflow` (`player-<hash>`) is the person. The workflow ID is a hash
  of the email. Name, title, email, consent, games played, and their single
  best score live here. The same email can start any number of fresh sessions.
- `LeaderboardWorkflow` (`leaderboard`) is one long-lived workflow for the
  standings. It keeps one best entry per player and continue-as-news after a
  couple hundred scores.

The game itself is `GameSessionWorkflow` (`session-<id>`) on task queue
`session-<id>`. Only that session's pod polls the queue. A crash
schedules `DeleteSessionPod` back on the control queue. That activity writes
the current pod UID (or process ID) into a heartbeat, deletes that generation,
and returns only after that pod is actually gone. On Kubernetes it then waits
`RECOVERY_DELAY` before returning, so the turn stays shut while the replacement
pod replays the workflow. A retry of the delete cannot kill the replacement,
because the heartbeat still names the generation that already died.

Queries need a live session worker. Until the replacement is polling, the
browser keeps the last state and keeps drawing the clock from the
deadline the workflow already recorded. The crash call returns as soon as it
has been waiting longer than a second, and the page freezes the flight
until the workflow is playing again. Gates you already cleared stay cleared.

`RUNTIME=process` is the same shape without Kubernetes: the control worker
spawns a child process per session and restarts it `RECOVERY_DELAY` after it
exits. `RUNTIME=kubernetes` creates a one-replica Deployment per session and
deletes the running pod.

## Run it against the dev server, no cluster

This is the path that does not need a registry or Temporal Cloud.

```bash
docker run -d --name podpocalypse-temporal -p 7233:7233 -p 8233:8233 \
  temporalio/temporal:1.5.1 \
  server start-dev --ip 0.0.0.0 --port 7233 --ui-port 8233

go run ./cmd/podpocalypse
```

Open http://127.0.0.1:8080. The Temporal Web UI is http://127.0.0.1:8233.
Set `TEMPORAL_UI_URL=http://127.0.0.1:8233` if you want the in-game link.

Leads are appended to `LEAD_PATH` (one JSON object per finished game) and also
kept on the player workflow. `GET /api/prize` returns the winner's email when
`ADMIN_TOKEN` is set and the request sends `Authorization: Bearer <token>`.
Without the token the route is not there, so the booth network cannot list
addresses. The public board shows name, title, and score.

## Minikube, still with the dev server

The dev server runs in the cluster, so the game does not need the internet
after the two images are loaded.

```bash
make image
minikube image load podpocalypse:dev
minikube image load temporalio/temporal:1.5.1
kubectl apply -f deploy/k8s/
kubectl -n podpocalypse rollout status deploy/temporal-dev --timeout=180s
kubectl -n podpocalypse rollout status deploy/booth --timeout=180s
minikube service booth -n podpocalypse --url
minikube service temporal-ui -n podpocalypse --url
```

`make minikube` does the image load and apply. Finished games are in
`/data/leads.jsonl` inside the booth pod. Session deployments older than 20
minutes are deleted by the booth.

## Temporal Cloud

Use the same image. Skip `deploy/k8s/temporal-dev.yaml`. Create a secret named
`temporal-cloud` with key `api-key`, and apply the environment in
`deploy/cloud/booth-env.yaml` after filling in the namespace address and image.
An API key turns on TLS. mTLS uses `TEMPORAL_TLS_CERT` and `TEMPORAL_TLS_KEY`
instead.

Session pods get the address, namespace, and API key from the control worker's
memory when the deployment is created. Those values are not workflow input, so
they are not written into history.

## Tests

```bash
go test ./...
go test -tags e2e -count=1 -timeout 4m ./e2e
```

The e2e test starts the dev server in Docker, plays a game through the HTTP
API, checks that a crash leaves the session with no worker, then checks
the leaderboard, the prize route, and the lead file.
