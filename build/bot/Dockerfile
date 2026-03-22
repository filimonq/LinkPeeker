FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o bot ./cmd/bot

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/bot .
COPY --from=builder /app/database ./database
CMD ["./bot"]