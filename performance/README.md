# Orbital Performance Testing

This directory contains performance testing tools for the Orbital library. It provides comprehensive benchmarking capabilities with automated result collection, profiling, and metrics visualization.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Setup and Deployment](#setup-and-deployment)
- [Running Performance Tests](#running-performance-tests)
- [Metrics and Monitoring](#metrics-and-monitoring)
- [Profiling](#profiling)
- [Cleanup](#cleanup)

## Prerequisites

Ensure you have the following installed and configured:

- Go
- Docker for building images
- Kubernetes cluster running and `kubectl` configured to access it
- Make utility

## Setup and Deployment

Run the following command to deploy the application:

```bash
make build && make deploy
```

For minikube:
```bash
make build && make minikube-load && make deploy
```

## Running Performance Tests

Before running the performance test, ensure you port-forward the manager service to your local machine:

```bash
make port-forward-manager
```

or if you are using minikube:

```bash
minikube tunnel
```

The performance test behavior is controlled through the [`parameters.json`](./parameters.json) file. Give the test a meaningful name by changing the `TestName` field. You could e.g. include the version of Orbital you are testing.

Use the following command to run a performance test:

```bash
make run-test
```

It will automatically create a new entry in [`/results`](./results) with the timestamp and the test name you provided.
The results include the business metrics, most importantly the time it took to complete the test.
You can also see the parameters you provided again.
Besides the business metrics, the results also include the profiles for CPU and memory usage of the manager during the test run.

## Metrics and Monitoring

You can also deploy the Prometheus and Grafana stack:

```bash
make deploy-metrics
```

Follow the instructions in [`./grafana-dashboard/README.md`](./grafana-dashboard/README.md) to set up the Grafana dashboards.

Before accessing the Grafana dashboard, you need to port-forward the Grafana service:

```bash
make port-forward-grafana
```

The dashboards provide comprehensive monitoring of your test environment, including PostgreSQL database performance metrics, Go runtime statistics, and real-time status tracking of jobs and tasks throughout the test execution.

## Profiling

After a successful test run, you can find the profiles in the [`results/<timestamp>-<testname>/`](./results) directory.

For visualizing the profiles, you can use the `pprof` tool:

```bash
go tool pprof -http=:8080 results/<timestamp>-<testname>/cpu/mem.prof
```

## Cleanup

To clean up the deployed resources, run:

```bash
make undeploy && make undeploy-metrics
``` 
