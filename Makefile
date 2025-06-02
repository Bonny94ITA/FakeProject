.PHONY: all install lint lint-fix test test-code-quality test-hash-client test-data-quality test-invalid-data run clean docker-build docker-run docker-test prod

# Development: all
all: install lint test run

# Production: no tests, optimized for runtime
prod: install lint run

# Quick: just the pipeline
quick: run

install:
	pip install --upgrade pip
	pip install -r requirements.txt

lint:
	ruff check src tests

lint-fix:
	ruff check --fix src tests

test:
	PYTHONPATH=. pytest tests --maxfail=1 --disable-warnings -q

test-code-quality:
	PYTHONPATH=. pytest tests/test_code_quality.py -v

test-data-quality:
	PYTHONPATH=. pytest tests/test_data_quality.py -v

test-hash-client:
	PYTHONPATH=. pytest tests/test_hash_client.py -v

run:
	PYTHONPATH=. python src/main.py

clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .pytest_cache

# Docker commands
docker-build:
	docker build -t data-transformer .

# Development: all
docker-run-with-tests:
	docker-compose run --rm app make all

# Production: no tests, optimized for runtime
docker-run:
	docker-compose up --build app

# Quick run: just the pipeline
docker-run-quick:
	docker-compose run --rm app make quick

# All test commands use dedicated test container
docker-test:
	docker-compose --profile test run --rm test make test

docker-test-code-quality:
	docker-compose --profile test run --rm test make test-code-quality

docker-test-data-quality:
	docker-compose --profile test run --rm test make test-data-quality

docker-test-hash-client:
	docker-compose --profile test run --rm test make test-hash-client

docker-shell:
	docker-compose --profile test run --rm test bash
