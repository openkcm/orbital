
.PHONY: default

minikube-init:
	(minikube start --driver=docker)

minikube-delete:
	(minikube delete)


build: build-manager

build-manager:
	GOOS=linux GOARCH=amd64 go build -o cmd/manager/manager cmd/manager/main.go
	chmod +x cmd/manager/manager
	(docker build -f cmd/manager/Dockerfile -t localhost/manager:latest .)
	(minikube image rm localhost/manager:latest)
	(minikube image load localhost/manager:latest)
	rm cmd/manager/manager

deploy:
	kubectl apply -f infrastructure.yaml

undeploy:
	kubectl delete -f infrastructure.yaml
