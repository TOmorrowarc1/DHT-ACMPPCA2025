FROM golang:1.18-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o dht .

FROM alpine:3.17
RUN apk add --no-cache iproute2
WORKDIR /app
COPY --from=builder /app/dht .
EXPOSE 20000
ENTRYPOINT ["./dht"]