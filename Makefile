.PHONY: lint check

# Run formatting + linting + type checking
init_linux:
	sudo apt update
	sudo apt install python3.12-venv
	sudo apt install python3-pip
	python3 -m venv .venv 
	. .venv/bin/activate
	pip3 install -r requirements.txt
	pip3 install -r lint_tool.txt	

clean:
	rm -rf .venv

lint:
	black .
	isort .
	flake8 demo
	pylint demo
	mypy

RAY_FILE ?= k8s/ray-job.yaml
deploy_ray:
	docker build -t ray-actor-example:latest . && kubectl apply -f $(RAY_FILE)