.PHONY: lint check

# Run formatting + linting + type checking
lint:
	black .
	isort .
	flake8 demo
	pylint demo
	mypy

# Run everything but fail if formatting is needed (good for CI)
check:
	black --check .
	isort --check-only .
	flake8
	pylint demo
	mypy
