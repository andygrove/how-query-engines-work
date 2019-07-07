FROM rustlang/rust:nightly AS build

USER root

#RUN apt update && apt -y install musl musl-dev musl-tools libssl-dev openssl
#
## Because we're using musl, we need to compile OpenSSL from source and statically link against it
#WORKDIR /tmp
#RUN curl -o openssl-1.0.2s.tar.gz https://www.openssl.org/source/openssl-1.0.2s.tar.gz && \
#    tar xzf openssl-1.0.2s.tar.gz && \
#    cd openssl-1.0.2s && \
#    ./config --openssldir=/usr/local/ssl && \
#    make install
#
## Download the target for static linking.
#RUN rustup target add x86_64-unknown-linux-musl

# Copy Ballista sources
RUN mkdir -p /tmp/ballista/src
RUN mkdir -p /tmp/ballista/proto
COPY Cargo.toml /tmp/ballista
COPY Cargo.lock /tmp/ballista
COPY build.rs /tmp/ballista
COPY src /tmp/ballista/src
COPY proto /tmp/ballista/proto

# compile
WORKDIR /tmp/ballista
RUN cargo install --path .
#RUN cargo install --target x86_64-unknown-linux-musl --path .

# Copy the statically-linked binary into a scratch container.
#FROM scratch
#COPY --from=build /usr/local/cargo/bin/ballista-server .
#USER 1000

EXPOSE 50051

CMD ["ballista-server"]
