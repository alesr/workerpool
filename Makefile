.DEFAULT_GOAL := help

PROJECT_NAME := workerpool

.PHONY: help
help:
	@echo "------------------------------------------------------------------------"
	@echo "${PROJECT_NAME}"
	@echo "------------------------------------------------------------------------"
	@awk 'BEGIN {FS = ":.*?## "}; $$0 ~ "^[[:alnum:]_/%-]+:.*?## " {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST) | sort

.PHONY: fmt
fmt: ## Format code
	go fmt ./...

.PHONY: vet
vet: ## Vet code
	go vet ./...

.PHONY: test
test: ## Run unit tests
	go test -short -race -count=1 -v ./...

.PHONY: lint
lint: vet ## Lint code
	@if command -v staticcheck >/dev/null 2>&1; then \
		staticcheck ./...; \
	else \
		echo "staticcheck not installed, skipping (go install honnef.co/go/tools/cmd/staticcheck@latest)"; \
	fi
