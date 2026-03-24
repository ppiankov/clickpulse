FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -ldflags "-X main.version=${VERSION}" -o /clickpulse ./cmd/clickpulse

FROM alpine:3.20
RUN apk --no-cache add ca-certificates
COPY --from=builder /clickpulse /usr/local/bin/clickpulse
EXPOSE 9188
ENTRYPOINT ["clickpulse"]
CMD ["serve"]
