
.PHONY: default

minikube-init:
	(minikube start --driver=docker)

minikube-delete:
	(minikube delete)


build: build-manager

build-manager:
	GOOS=linux GOARCH=arm64 go build -o cmd/manager/manager cmd/manager/*.go
	chmod +x cmd/manager/manager
	(docker build -f cmd/manager/Dockerfile -t localhost/manager:latest .)
	(minikube image rm localhost/manager:latest)
	(minikube image load localhost/manager:latest)
	rm cmd/manager/manager

deploy:
	kubectl apply -f orbital-run.yaml

undeploy:
	kubectl delete -f orbital-run.yaml

deploy-metrics:
	kubectl apply -f metrics.yaml

undeploy-metrics:
	kubectl delete -f metrics.yaml
