.PHONY: test e2e image minikube

test:
	go test ./...

e2e:
	go test -tags e2e -count=1 -timeout 4m ./e2e

image:
	CGO_ENABLED=0 GOOS=linux GOARCH=$$(go env GOARCH) go build -o podpocalypse ./cmd/podpocalypse
	docker build -t podpocalypse:dev .

minikube: image
	minikube image load podpocalypse:dev
	minikube image load temporalio/temporal:1.5.1
	kubectl apply -f deploy/k8s/
	kubectl -n podpocalypse rollout status deploy/temporal-dev --timeout=180s
	kubectl -n podpocalypse rollout status deploy/booth --timeout=180s
	minikube service booth -n podpocalypse --url
