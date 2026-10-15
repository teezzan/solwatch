# syntax=docker/dockerfile:1
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# modernc.org/sqlite is pure Go, so the binary is fully static.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/solwatch ./cmd/solwatch \
 && mkdir -p /out/data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/solwatch /solwatch
# distroless has no shell to mkdir/chown with, so bring a writable /data along.
COPY --from=build --chown=65532:65532 /out/data /data
ENV LISTEN_ADDR=:8080 \
    DB_PATH=/data/solwatch.db
VOLUME ["/data"]
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/solwatch"]
