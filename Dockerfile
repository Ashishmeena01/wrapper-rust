# Build stage
FROM rust:1-bookworm AS builder
WORKDIR /app
# Copy the manifest and lockfile first so dependency resolution is reproducible.
# Without Cargo.lock, Cargo picks the newest crates, which currently require a
# rustc newer than the one in older images and break the build.
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked


# Runtime stage
FROM debian:bookworm-slim
# Copy the CA bundle from the builder instead of running apt-get.
# The apt-get install needs network access to the Debian mirrors, which is
# flaky in some environments (DNS failures), and it failed the whole build.
# The builder image (buildpack-deps based) already ships ca-certificates.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
# useradd is provided by the `passwd` package, which is already in the base image.
RUN useradd --system --uid 10001 --create-home appuser
WORKDIR /app
COPY --from=builder /app/target/release/competition-api /usr/local/bin/competition-api
USER appuser
EXPOSE 8080
CMD ["competition-api"]
