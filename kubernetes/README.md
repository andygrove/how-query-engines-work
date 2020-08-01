# Deploying Ballista to Kubernetes

This document explains how to create a Ballista cluster in Kubernetes.

## Generic kubernetes

This guide assumes that we have a kubernetes cluster and our role has permissions to:

* list and create pods
* list and create secrets
* list and create persistent volumes and claims
* create ClusterRole and ClusterRoleBinding

on the namespace `default`.

### Deploy volumes and write data to it

For this guide, we will create a persistent volume and claim and write data to it, which we can then query through Ballista.
To do so, run

```bash
kubectl apply -f kubernetes/pv.yaml
```

and check that its status and the claim status is "Bound":

```
kubectl get pv
kubectl get pvc
```

Next, we will spawn a pod to download data from s3 and write to the persistent volume:

```bash
kubectl apply -f kubernetes/populate-data.yaml
```

This operation takes some time as we have some data to download. This is done when the pod `populate-data` on `kubectl get pods`
shows status `Completed`. We can't deploy the cluster before this step is completed, as the volume can only be claimed by
a single pod at the time. Feel free to change [populate-data.yaml](./populate-data.yaml) do dump a smaller CSV.

### Setup credentials for pulling images

We offer docker images on github that we can use to run the cluster.
They require authentication to be pulled. For this, we will create a secret in k8s:

```bash
kubectl create secret docker-registry github \
    --docker-server=docker.pkg.github.com \
    --docker-username=$GITHUB_USERNAME \
    --docker-password=$GITHUB_TOKEN \
    --docker-email=jorgecarleitao@gmail.com
```

where `$GITHUB_TOKEN` is an [OAuth token](https://docs.github.com/en/github/authenticating-to-github/creating-a-personal-access-token) for your `$USERNAME`.

### Give the cluster permissions to list pods

```bash
kubectl apply -f kubernetes/rbac.yaml
```

This gives all pods running on namespace `default` permissions to list pods from the cluster's API
and is necessary for scheduling.

### Deploy Ballista

Once `populate-data` completes, we can bring the cluster up. For this, run

```bash
kubectl apply -f kubernetes/ballista-cluster.yaml
```

We can check the status with `kubectl get pods` and `kubectl logs ballista-0`.
This deploys two replicas and a service on port `50051`.
At this point, we can connect remotely with this port to run queries against the cluster.

A simple method to connect to it is to port-forward to our local machine:

```bash
kubectl port-forward service/ballista 50051:50051
```

and connect to this port to run queries against the cluster. In particular, 
we can use [this example](../rust/examples/distributed-query) to query the cluster.

### Customization

The procedure above is a minimal setup to deploy ballista on kubernetes. From here, we can just change any configuration above
to our needs. In particular, we can use the image name from `ballistacompute/ballista-rust` to `ballistacompute/ballista-jvm` or `ballistacompute/ballista-spark` to use a different executor.

## Deploy on minikube

For local development, we can also use minikube. We can set it up exactly the same way, but since 
minikube runs locally, we can simplify the deployment a bit:

First, we can use docker images from our own docker deamon,

```bash
eval $(minikube -p minikube docker-env)
minikube start --driver=docker
```

which does not require setting up a github access token.

Secondly, we can use a local mounted volume before creating a persistent volume:

```bash
minikube mount /mnt/:/mnt/
```

This way, we can download the data to our computer and place it in the mounted volume.

## Teardown

To revert all the steps above, we run `kubectl delete -f` on each of the files above. To delete the secret used
to download images, run

```bash
kubectl delete secret github
```
