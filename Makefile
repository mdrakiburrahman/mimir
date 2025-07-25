PHONY: help install install-dev test test-e2e test-e2e-manual test-all coverage build validate style example-up example-down clean

# Define the virtual environment directory
VENV_DIR := .venv

help:
	@echo "Mimir Makefile"
	@echo "----------------"
	@echo "Available commands:"
	@echo "  install         - Create a venv and install production dependencies."
	@echo "  install-dev     - Create a venv, install all dependencies (dev, test), and pre-commit hooks."
	@echo "  test            - Run the test suite (excluding e2e tests)."
	@echo "  test-e2e        - Start services and run end-to-end tests."
	@echo "  test-e2e-manual - Run end-to-end tests on already running services."
	@echo "  test-all        - Run all tests (unit and e2e)."
	@echo "  coverage        - Run tests and generate a coverage report."
	@echo "  build           - Build the project for distribution."
	@echo "  validate        - Validate the configuration files."
	@echo "  style           - Run all pre-commit hooks on the codebase."
	@echo "  example-up      - Start the example application using Docker Compose."
	@echo "  example-down    - Stop the example application."
	@echo "  clean           - Remove temporary files and build artifacts."

install:
	@if [ ! -d "$(VENV_DIR)" ]; then \
		echo "Creating virtual environment with uv..."; \
		uv venv $(VENV_DIR); \
	fi
	@echo "Installing production dependencies with uv..."
	@uv pip install -e .

install-dev:
	@if [ ! -d "$(VENV_DIR)" ]; then \
		echo "Creating virtual environment with uv..."; \
		uv venv $(VENV_DIR); \
	fi
	@echo "Installing all dependencies with uv..."
	@uv pip install -e ".[all]"
	@echo "Installing pre-commit hook..."
	@uv run pre-commit install

test:
	@echo "Running tests (excluding e2e)..."
	@uv run pytest -m "not e2e"

test-e2e:
	@echo "Running end-to-end tests (with service management)..."
	@make example-up
	@uv run pytest -m e2e
	@make example-down

test-e2e-manual:
	@echo "Running end-to-end tests (manual)..."
	@uv run pytest -m e2e

test-all: test test-e2e

coverage:
	@echo "Running tests with coverage..."
	@uv run pytest --cov=src/mimir

build:
	@echo "Building project..."
	@rm -rf dist build
	@uv run python -m build

validate:
	@echo "Validating configuration..."
	@uv run mimir validate

style:
	@echo "Running style checks with pre-commit..."
	@uv run pre-commit run --all-files

example-up:
	@echo "Starting example application..."
	@docker-compose -f example/docker-compose.yaml up --build -d

example-down:
	@echo "Stopping example application..."
	@docker-compose -f example/docker-compose.yaml down

clean:
	@echo "Cleaning up..."
	@rm -rf .pytest_cache .ruff_cache build dist src/mimir.egg-info
	@find . -type f -name '*.pyc' -delete
	@find . -type d -name '__pycache__' -delete
