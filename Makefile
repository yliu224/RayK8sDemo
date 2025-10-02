.PHONY: lint check

# Run formatting + linting + type checking

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
	sudo docker build -t raydemo.azurecr.io/ray-actor-example:latest . && sudo docker push raydemo.azurecr.io/ray-actor-example:latest && kubectl apply -f $(RAY_FILE)

init_linux:
	# Install environment
	sudo apt update
	sudo apt install python3.12-venv
	sudo apt install python3-pip
	python3 -m venv .venv 
	. .venv/bin/activate
	pip3 install -r requirements.txt
	pip3 install -r lint_tool.txt

	# Install developer tools
	sudo snap install docker
	curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash


init_linux_k8s:
	# Install MicroK8s
	sudo snap install microk8s --classic
	sudo usermod -aG microk8s $USER
	newgrp microk8s

	# kubectl + Helm
	sudo apt-get install -y kubectl
	sudo snap install helm --classic
	sudo microk8s config > ~/.kube/config

	# K9s
	wget https://github.com/derailed/k9s/releases/latest/download/k9s_linux_amd64.deb && apt install ./k9s_linux_amd64.deb && rm k9s_linux_amd64.deb
	

install_ray:
	helm repo add kuberay https://ray-project.github.io/kuberay-helm/
	helm repo update
	helm install kuberay-operator kuberay/kuberay-operator --version 1.4.2

LOCAL_CONNECTION_STR ?= $LOCAL_CONNECTION_STR
DX_API_TOKEN ?= $DX_API_TOKEN
install_secrets:
	kubectl create secret generic ray-connection-str --from-literal=RAY_CONNECTION_STR=$(LOCAL_CONNECTION_STR)
	kubectl create secret generic dxpy-api-secret --from-literal=DX_API_TOKEN=$(DX_API_TOKEN)


ACR_NAME ?= raydemo
link_acr:
	@ACR_LOGIN_SERVER=$$(az acr show --name $(ACR_NAME) --query loginServer -o tsv); \
	ACR_USERNAME=$$(az acr credential show --name $(ACR_NAME) --query username -o tsv); \
	ACR_PASSWORD=$$(az acr credential show --name $(ACR_NAME) --query "passwords[0].value" -o tsv); \
	kubectl delete secret acr-secret --ignore-not-found; \
	kubectl create secret docker-registry acr-secret \
		--docker-server=$$ACR_LOGIN_SERVER \
		--docker-username=$$ACR_USERNAME \
		--docker-password=$$ACR_PASSWORD \
		--docker-email=unused@example.com; \
		
	kubectl patch serviceaccount default \
		-p "{\"imagePullSecrets\": [{\"name\": \"acr-secret\"}]}"; \