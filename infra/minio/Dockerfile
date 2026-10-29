# Build the pinned upstream source: historical public images are no longer reliable.
FROM golang:1.23-bookworm AS build
RUN git clone --depth 1 --branch RELEASE.2024-12-18T13-15-44Z https://github.com/minio/minio.git /src
WORKDIR /src
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /minio .

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/* \
    && useradd -u 10002 -m minio && mkdir /data && chown minio:minio /data
COPY --from=build /minio /usr/local/bin/minio
USER minio
ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]
