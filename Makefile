# MongoDB Log Analyzer - Development Makefile

.PHONY: help install test test-verbose test-coverage clean dev-setup lint format check-security run

# Default target
help:
	@echo "MongoDB Log Analyzer - Available Commands"
	@echo "========================================="
	@echo "install         Install dependencies"
	@echo "dev-setup       Setup development environment"
	@echo "test            Run all tests"
	@echo "test-verbose    Run tests with verbose output"
	@echo "test-coverage   Run tests with coverage report"
	@echo "lint            Run code linting"
	@echo "format          Format code with black"
	@echo "check-security  Run security checks"
	@echo "clean           Clean temporary files"
	@echo "run             Start the application"

# Installation and setup
install:
	@echo "📦 Installing dependencies..."
	@python -m pip install --upgrade pip
	@pip install flask coverage bandit black flake8 pre-commit
	@if [ -f requirements.txt ]; then pip install -r requirements.txt; fi

dev-setup: install
	@echo "🔧 Setting up development environment..."
	@if [ ! -d "test_env" ]; then python -m venv test_env; fi
	@chmod +x run_tests.sh
	@pre-commit install
	@echo "✅ Development environment ready!"

# Testing
test:
	@echo "🧪 Running automated tests..."
	@./run_tests.sh

test-verbose:
	@echo "🧪 Running tests with verbose output..."
	@./run_tests.sh --verbose

test-coverage:
	@echo "🧪 Running tests with coverage analysis..."
	@./run_tests.sh --coverage

# Code quality
lint:
	@echo "🔍 Running code linting..."
	@flake8 app.py test_framework.py --max-line-length=100 --ignore=E203,W503

format:
	@echo "✨ Formatting code..."
	@black app.py test_framework.py --line-length=100

check-security:
	@echo "🔒 Running security checks..."
	@bandit -r . -f json -o bandit-report.json
	@echo "Security report saved to bandit-report.json"

# Application
run:
	@echo "🚀 Starting MongoDB Log Analyzer..."
	@python app.py

# Maintenance
clean:
	@echo "🧹 Cleaning temporary files..."
	@rm -rf __pycache__/
	@rm -rf .pytest_cache/
	@rm -rf htmlcov/
	@rm -f .coverage
	@rm -f coverage.xml
	@rm -f test_report.json
	@rm -f bandit-report.json
	@find . -type f -name "*.pyc" -delete
	@find . -type d -name "__pycache__" -delete
	@echo "✅ Cleanup complete!"

# CI targets
ci-test: install test-coverage

# Development workflow
dev: dev-setup format lint test-verbose
	@echo "🎉 Development workflow complete!"