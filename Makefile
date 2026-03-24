VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
VERSION_NUM = $(shell echo $(VERSION) | sed 's/^v//')
LDFLAGS = -ldflags "-X main.version=$(VERSION_NUM)"

.PHONY: build test lint clean docker

build:
	go build $(LDFLAGS) -o bin/clickpulse ./cmd/clickpulse

test:
	go test -race -count=1 ./...

lint:
	golangci-lint run ./...

clean:
	rm -rf bin/

docker:
	docker build -t clickpulse:dev .

.DEFAULT_GOAL := build
