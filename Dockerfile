# syntax=docker/dockerfile:1.7

ARG BUILDPLATFORM
ARG TARGETPLATFORM

FROM --platform=$BUILDPLATFORM node:22-bookworm-slim AS dashboard-builder
WORKDIR /workspace/dashboard

COPY dashboard/package*.json ./
RUN npm ci

COPY dashboard/ ./
RUN npm run build

FROM --platform=$BUILDPLATFORM rust:1-bookworm AS backend-builder
WORKDIR /workspace

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        clang \
        cmake \
        git \
        libssl-dev \
        libsasl2-dev \
        pkg-config \
        zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY migrations ./migrations
COPY static ./static

RUN cargo build --release --locked --bin db-con

FROM debian:bookworm-slim AS runtime
WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libgcc-s1 \
        libssl3 \
        libsasl2-2 \
        zlib1g \
    && rm -rf /var/lib/apt/lists/*

COPY --from=backend-builder /workspace/target/release/db-con ./db-con
COPY --from=backend-builder /workspace/static ./static
COPY --from=dashboard-builder /workspace/dashboard/dist ./static/dist

EXPOSE 3000

CMD ["./db-con"]
