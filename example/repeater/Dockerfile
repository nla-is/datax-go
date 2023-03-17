FROM golang:1.20-alpine3.17 AS builder

WORKDIR /workspace

COPY go.mod go.mod
COPY go.sum go.sum

RUN go mod download

COPY main.go main.go

RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 go build -o /repeater .

FROM alpine:3.17

WORKDIR /

COPY --from=builder /repeater /repeater

ENTRYPOINT ["/repeater"]
