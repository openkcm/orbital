
.PHONY: default

minikube-init:
	(minikube start --driver=docker)

minikube-delete:
	(minikube delete)

minikube-load:
	(minikube image rm localhost/manager:latest)
	(minikube image load localhost/manager:latest)

build: build-manager

build-manager:
	GOOS=linux GOARCH=arm64 go build -o cmd/manager/manager cmd/manager/*.go
	chmod +x cmd/manager/manager
	(docker build -f cmd/manager/Dockerfile -t localhost/manager:latest .)
	rm cmd/manager/manager

deploy:
	kubectl apply -f manager.yaml

undeploy:
	kubectl delete -f manager.yaml

deploy-metrics:
	kubectl apply -f metrics.yaml

undeploy-metrics:
	kubectl delete -f metrics.yaml

port-forward-manager:
	kubectl port-forward svc/manager-service 8080:8080

port-forward-postgres:
	kubectl port-forward svc/postgres-service 5432:5432

port-forward-grafana:
	kubectl port-forward svc/grafana-service 3000:3000

run-test:
	@echo "Running performance test..."
	@curl -OJ -X POST --header "Content-Type: application/json" --connect-timeout 10240 --max-time 10240 --data '@parameters.json' http://localhost:8080/orbital/start
	@echo "Processing downloaded zip file..."
	@mkdir -p results
	@ZIPFILE=$$(ls -t *.zip 2>/dev/null | head -n1); \
	if [ -n "$$ZIPFILE" ]; then \
		echo "Found zip file: $$ZIPFILE"; \
		BASENAME=$$(basename "$$ZIPFILE" .zip); \
		mv "$$ZIPFILE" results/; \
		mkdir -p "results/$$BASENAME"; \
		cd "results/$$BASENAME" && unzip "../$$ZIPFILE"; \
		echo "Extracted to results/$$BASENAME"; \
		rm ../$$ZIPFILE; \
	else \
		echo "No zip file found"; \
	fi
	