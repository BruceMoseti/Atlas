# syntax=docker/dockerfile:1

FROM golang:1.22-alpine AS build
WORKDIR /src

# Dependencies first, so a source-only change does not re-download them.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO_ENABLED=0 works because Atlas uses modernc.org/sqlite, a pure-Go SQLite.
# That is the reason the runtime image can be a plain alpine with no libsqlite.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/atlas-server ./cmd/atlas-server && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/atlas-worker ./cmd/atlas-worker && \
    CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/atlas        ./cmd/atlas-cli

FROM alpine:3.20
# wget for the compose healthcheck; the shell is what the example workloads run.
RUN apk add --no-cache ca-certificates
COPY --from=build /out/atlas-server /usr/local/bin/atlas-server
COPY --from=build /out/atlas-worker /usr/local/bin/atlas-worker
COPY --from=build /out/atlas        /usr/local/bin/atlas
RUN mkdir -p /data
WORKDIR /data
EXPOSE 50051 9090
ENTRYPOINT []
CMD ["atlas-server"]
