# Use the official Rust image as a builder stage to compile your application
FROM rust:latest as builder

# Install Python 3.11 and other dependencies
RUN apt-get update && apt-get install -y \
    python3.11 python3.11-venv python3.11-dev \
    libgstreamer1.0-dev \
    libgstreamer-plugins-base1.0-dev \
    pkg-config \
 && rm -rf /var/lib/apt/lists/*

# Setup to use Python 3.11 as the default python3
RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1

# Create a new empty shell project
RUN USER=root cargo new --bin app
WORKDIR /app

# Copy your manifests
COPY ./Cargo.lock ./Cargo.lock
COPY ./Cargo.toml ./Cargo.toml

RUN apt-get update && apt-get install -y \
    libgstreamer1.0-dev \
    libgstreamer-plugins-base1.0-dev \
    pkg-config \
 && rm -rf /var/lib/apt/lists/*



# This trick will cache your dependencies to not rebuild them on every build
RUN cargo build --release
RUN rm src/*.rs

# Copy your source tree
COPY ./src ./src

# Build for release.
RUN rm -rf ./target/release/*
RUN cargo build --release

# Final stage: use the official Debian slim image for a smaller footprint
FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    libgstreamer1.0-0 \
    gstreamer1.0-plugins-base \
    gstreamer1.0-plugins-good \
    gstreamer1.0-plugins-bad \
    gstreamer1.0-plugins-ugly \
    gstreamer1.0-libav \
    libgstrtspserver-1.0-0  \
  && rm -rf /var/lib/apt/lists/* \

# Copy the binary from the builder stage
COPY --from=builder /app/target/release/pipeless-ai /usr/local/bin/app

# Run the binary
CMD ["app"]
