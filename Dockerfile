FROM golang:1.21-alpine as builder

ADD . /koinos-token-tracker
WORKDIR /koinos-token-tracker

RUN go get ./... && \
    go build -ldflags="-X main.Commit=$(git rev-parse HEAD 2>/dev/null || echo unknown)" \
             -o koinos_token_tracker cmd/koinos-token-tracker/main.go

FROM alpine:latest
COPY --from=builder /koinos-token-tracker/koinos_token_tracker /usr/local/bin
ENTRYPOINT [ "/usr/local/bin/koinos_token_tracker" ]
