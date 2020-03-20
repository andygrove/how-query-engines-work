# Ballista Example: Parallel Aggregate Query in Rust

*NOTE: this is a work-in-progress and is not functional yet*

This example shows how to manually create a Ballista cluster of Rust executors and run an aggregate query in parallel across those executors.

## Prerequisites


You will need a Kubernetes cluster to deploy to. I am using Minikube.

```bash
sudo minikube start --vm-driver=podman
```

- Sample data installed (TBD)

## Step 1: Build Docker Image for Ballista Rust Project

From the root of the project.

```bash
docker build -t ballistacompute/rust-base -f docker/rust-base.dockerfile .
docker build -t ballistacompute/rust -f docker/rust.dockerfile .
```

## Step 2: Build Docker Image for Example

From this directory.

```bash
./build-docker-image.sh
```

## Step 3: Create Ballista cluster in k8s

```bash
kubectl apply -f cluster-deployment.yaml
```

## Step 4: Deploy Example


## Teardown

Remove cluster:

```bash
kubectl delete -f cluster-deployment.yaml
```