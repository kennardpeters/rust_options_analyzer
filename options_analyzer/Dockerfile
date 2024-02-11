FROM rust:1.72.0 as builder
WORKDIR /usr/src/options_analyzer
COPY . .
RUN cargo install --path .
FROM debian:buster-slim
RUN apt-get update & apt-get install -y curl
RUN apt-get update & apt-get install -y extra-runtime-dependencies & rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/local/cargo/bin/options_analyzer /usr/local/bin/options_analyzer
CMD ["options_analyzer"]

