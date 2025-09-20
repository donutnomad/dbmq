# Repository Guidelines

## Project Structure & Module Organization
Core Go services for the message queue live in the root package (`admin.go`, `producer.go`, `consumer.go`, etc.) and share domain types from `data.go` and `metrics.go`. Persistence logic sits in `internal/dao`, schema setup in `internal/db`, interfaces in `internal/interfaces`, and logging helpers in `logger/`. Reference docs are collected in `docs/`, while runnable samples live in `examples/` (`rest_api_demo`, `super_demo`). The Dashboard UI is a Next.js app in `dashboard-ui/`; treat it as a separate Node workspace.

## Build, Test, and Development Commands
- `go build ./...` — compile all Go services; run before opening a PR.
- `go test ./...` — execute unit tests, including coordinator and helper suites.
- `go test -tags=integration ./...` — exercises the MySQL + Redis flow; requires local MySQL (`root:123456@127.0.0.1:3306`) and Redis (`127.0.0.1:6379`, DB 2).
- `make super` — starts the end-to-end demo in `examples/super_demo`.
- `cd dashboard-ui && npm install && npm run dev` — boot the admin console; `npm run build` prepares a production bundle.

## Coding Style & Naming Conventions
Format Go code with `go fmt ./...` (Go 1.24) and keep imports sorted via `goimports`. Stick to idiomatic Go naming: exported symbols PascalCase, private identifiers camelCase, files snake_case. Prefer early returns and context-aware APIs (`context.Context`). Frontend code follows the Next.js/TypeScript defaults; enforce ESLint and Prettier through `npm run lint`. Commit generated assets like `dashboard-ui/out` only when explicitly used.

## Testing Guidelines
Add table-driven unit tests alongside implementations (`*_test.go`) and lean on `stretchr/testify` assertions already in use. Integration specs live in `integration_test.go` behind the `integration` build tag; document any external services they need and ensure teardown leaves MySQL/Redis clean. Include regression tests when touching coordinator or consumer logic.

## Commit & Pull Request Guidelines
The history uses Conventional Commit prefixes (`fix:`, `refactor:`); follow the same pattern and keep summaries under 65 characters. Each PR should explain the behavioral change, list verification steps (tests, demos), and link tracking issues. Provide screenshots or curl snippets when altering `rest_api.go` or `dashboard-ui`. Keep PRs focused; split sweeping refactors from feature work.
