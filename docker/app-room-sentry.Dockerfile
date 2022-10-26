FROM golang:1.18-alpine as builder

WORKDIR /ion

COPY go.mod go.mod
COPY go.sum go.sum
RUN go mod download

COPY pkg/ pkg/
COPY proto/ proto/
COPY apps/ apps/

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GO111MODULE=on go build -a -installsuffix cgo -o /ion/app-room-sentry ./apps/room-sentry

FROM alpine

COPY --from=builder /ion/app-room-sentry /app-room-sentry

COPY configs/docker/app-room-sentry.toml /configs/app-room-sentry.toml

ENTRYPOINT ["/app-room-sentry"]
CMD ["-c", "/configs/app-room-sentry.toml"]
