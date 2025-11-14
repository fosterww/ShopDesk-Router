SHELL := /bin/bash

.PHONY: dev down logs lint fmt test

dev: 
	docker compose up --build

down:
	docker compose down

logs:
	docker compose logs -f --tail=200

lint:
	docker run --rm -v $(PWD):/app -w /app python:3.12-slim bash -lc "pip install ruff && ruff check ."

fmt:
	docker run --rm -v $(PWD):/app -w /app python:3.12-slim bash -lc "pip install black && black ."

test:
	docker run --rm -v $(PWD):/app -w /app python:3.12-slim bash -lc "pip install -r requirements.txt && pytest -q"
