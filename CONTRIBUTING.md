# Contributing to clickpulse

1. Fork the repo
2. Create a feature branch (`git checkout -b feat/my-feature`)
3. Write tests for your changes
4. Run `make test` and `make lint`
5. Commit with conventional commit messages
6. Open a pull request against `main`

## Development

```bash
make build    # Build binary
make test     # Run tests with race detector
make lint     # Run golangci-lint
make docker   # Build Docker image
```

## Commit messages

Use conventional commits: `feat:`, `fix:`, `docs:`, `test:`, `refactor:`, `chore:`
