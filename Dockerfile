FROM rustlang/rust:nightly

RUN mkdir /opt/ballista

ADD Cargo.toml /opt/ballista
ADD src /opt/ballista
ADD proto /opt/ballista

WORKDIR /opt/ballista

RUN cargo build --release