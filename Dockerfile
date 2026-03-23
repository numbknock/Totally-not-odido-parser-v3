FROM golang:1.24-bookworm AS builder
WORKDIR /src
ARG TARGETOS
ARG TARGETARCH

COPY go.mod go.sum ./
RUN go mod tidy && go mod download

COPY cmd ./cmd
COPY internal ./internal
COPY static ./static

RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build -o /out/totally-not-odido-parser-v3 ./cmd/server

FROM alpine:3.20
WORKDIR /app

RUN addgroup -S app && adduser -S -G app app

COPY --from=builder /out/totally-not-odido-parser-v3 /app/totally-not-odido-parser-v3
COPY static /app/static

USER app
EXPOSE 8080

CMD ["/app/totally-not-odido-parser-v3", "-addr", ":8080", "-dataset", "/data/dataset.txt", "-db", "/data/dataset.sqlite"]
