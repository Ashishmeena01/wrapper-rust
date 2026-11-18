# Build stage
FROM rust:1.85-bookworm AS builder
WORKDIR /app
COPY Cargo.toml ./
COPY src ./src
RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim
# Copy the CA bundle from the builder instead of running apt-get.
# The slim image has no network access to mirrors during some builds
# (flaky DNS), and apt-get would fail the whole image build. The builder
# image (buildpack-deps based) already ships ca-certificates.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
# useradd is provided by the `passwd` package, which is already in the base image.
RUN useradd --system --uid 10001 --create-home appuser
WORKDIR /app
COPY --from=builder /app/target/release/competition-api /usr/local/bin/competition-api
USER appuser
EXPOSE 8080
CMD ["competition-api"]
