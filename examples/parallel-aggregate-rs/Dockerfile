FROM rustlang/rust:nightly

ADD temp/ /opt

ADD . /opt/examples/parallel-aggregate-rs/
WORKDIR /opt/examples/parallel-aggregate-rs

RUN cargo build --release

