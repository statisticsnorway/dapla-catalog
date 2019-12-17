FROM golang:1.13.5-alpine3.10 as builder

RUN apk update && \
    apk upgrade && \
    apk add git && \
    go get github.com/google/btree && \
    go get rsc.io/binaryregexp && \
    go get cloud.google.com/go/bigtable

RUN go build /go/src/cloud.google.com/go/bigtable/cmd/emulator/cbtemulator.go

FROM alpine:3.10

COPY --from=builder /go/cbtemulator /

ENTRYPOINT ["/cbtemulator", "-port", "9035", "-host", "0.0.0.0"]

EXPOSE 9035
