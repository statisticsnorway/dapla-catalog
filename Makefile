SHELL:=/usr/bin/env bash

.PHONY: default
default: | help

.PHONY: start-bigtable
start-bigtable: ## Start bigtable
	docker-compose up -d --build bigtable

.PHONY: stop-bigtable
stop-bigtable: ## Stop bigtable
	docker-compose rm --force --stop bigtable

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-45s\033[0m %s\n", $$1, $$2}'
