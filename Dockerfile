FROM rust:1.76.0-bookworm as builder

WORKDIR /usr/src/ord

# 安装必要的系统依赖项
RUN apt-get update && apt-get install -y \
    libclang-dev \
    clang \
    pkg-config \
    build-essential \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . .

#RUN cargo build --bin ord --release
RUN RUSTFLAGS="--deny warnings" cargo build --bin ord --release

FROM debian:bookworm-slim

COPY --from=builder /usr/src/ord/target/release/ord /usr/local/bin
RUN apt-get update && apt-get install -y openssl
WORKDIR /usr/local/bin
ENV RUST_BACKTRACE=1
ENV RUST_LOG=info
