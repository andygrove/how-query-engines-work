# Ballista Tutorial

This tutorial walks through running the included nyc taxi example.

# Set up environment

- Create a minikube cluster
- Pull ballista docker images

# Download data

```bash
./bin/download-yellow-2018.sh
```

# Create a Ballista cluster

```bash
docker run --network=host -it ballista /ballista create-cluster -n nyctaxi -e 12
```

# Run the app

```bash
docker run --network=host -it ballista /ballista run -n nyctaxi -i andygrove/ballista-nyctaxi:0.1.0
```




