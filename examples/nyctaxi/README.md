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
cargo run --bin ballista -- create-cluster -n nyctaxi -e 12
```

# Run the app

```bash
cargo run --bin ballista -- run -n nyctaxi -a andygrove/ballista-nyctaxi:0.1.0
```


# Delete the cluster

```bash
cargo run --bin ballista -- delete-cluster -n nyctaxi
```




