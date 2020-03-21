FROM ballistacompute/rust-cached-deps

ADD temp/ /opt

ADD . /opt/examples/parallel-aggregate-rs/
WORKDIR /opt/examples/parallel-aggregate-rs

RUN cargo build --release

ENTRYPOINT ["cargo run --release"]



