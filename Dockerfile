# silodb-server — standalone HTTP layer over silodb (the engine itself is
# an embeddable library; this image is one way to run it as a service).
FROM rust:1-slim AS build
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release -p silodb-server

FROM debian:bookworm-slim
RUN useradd --system --uid 65532 --create-home silodb \
    && mkdir -p /data && chown silodb /data
COPY --from=build /src/target/release/silodb-server /usr/local/bin/silodb-server
USER silodb
VOLUME /data
ENV SILODB_DB=/data/silodb.db \
    SILODB_ADDR=0.0.0.0:8080
EXPOSE 8080
CMD ["silodb-server"]
