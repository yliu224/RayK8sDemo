.PHONY: lint check

# Run formatting + linting + type checking
lint:
	black .
	isort .
	flake8 demo
	pylint demo
	mypy

RAY_FILE ?= k8s/ray-job.yaml
deploy_ray:
	docker build -t ray-actor-example:latest . && kubectl apply -f $(RAY_FILE)