# Build stage
FROM rust:slim-bookworm as builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Create a new empty project
WORKDIR /app
COPY . .

# Build the application in release mode
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    libssl-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the binary from the build stage
COPY --from=builder /app/target/release/mdc /usr/local/bin/mdc

# Copy the config file
COPY --from=builder /app/mdc.yaml /etc/mdc.yaml

# Set executable permissions
RUN chmod +x /usr/local/bin/mdc

# Run the application
CMD ["mdc", "--config", "/etc/mdc.yaml", "--log-level", "info"]
