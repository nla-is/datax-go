FROM debian:bookworm-slim

ARG TARGETARCH

WORKDIR /

COPY build/repeater.$TARGETARCH /repeater

ENTRYPOINT ["/repeater"]
