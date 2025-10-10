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

deploy_ray:
	RAY_FILE=k8s/$(STAGE)-job.yaml; \
	kubectl delete rayjob rayjob-$(STAGE) --ignore-not-found; \
	sudo docker build -t raydemo.azurecr.io/ray-actor-example:latest . && \
	sudo docker push raydemo.azurecr.io/ray-actor-example:latest && \
	kubectl apply -f $$RAY_FILE

init_linux:
	# Install python environment
	sudo apt update; \
	sudo apt install python3.12-venv; \
	sudo apt install python3-pip; \
	python3 -m venv .venv; \
	sleep 2; \
	. .venv/bin/activate; \
	pip3 install -r requirements.txt; \
	pip3 install -r lint_tool.txt; \

	# Install developer tools(docker + azure cli)
	sudo snap install docker; \
	curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash; \


install_k8s:
	# Install kind

	curl -Lo ./kind https://kind.sigs.k8s.io/dl/v0.30.0/kind-linux-amd64; \
	chmod +x ./kind; \
	sudo mv ./kind /usr/local/bin/kind; \
	mkdir /home/azureuser/.azure; \
	sudo kind get clusters | grep -qx kind && sudo kind delete cluster --name kind; \
	sudo kind create cluster --config k8s/local_credential/mount_path.yaml; \


	# kubectl + Helm
	sudo snap remove kubectl helm; \
	sudo snap install kubectl --classic; \
	sudo snap install helm --classic; \
	mkdir /home/azureuser/.kube/; \
	sudo kind get kubeconfig > /home/azureuser/.kube/config; \

	# K9s
	sudo wget https://github.com/derailed/k9s/releases/latest/download/k9s_linux_amd64.deb && sudo apt install ./k9s_linux_amd64.deb && sudo rm k9s_linux_amd64.deb; \


install_ray:
	helm repo add kuberay https://ray-project.github.io/kuberay-helm/; \
	helm repo update; \
	helm install kuberay-operator kuberay/kuberay-operator --version 1.4.2; \

# If you are using access_key, please use this connection string format:
# "DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
CONNECTION_STR ?= $$CONNECTION_STR
DX_API_TOKEN ?= $$DX_API_TOKEN
install_secrets:
	kubectl delete secret ray-connection-str dxpy-api-secret --ignore-not-found; \
	kubectl create secret generic ray-connection-str --from-literal=RAY_CONNECTION_STR="$(CONNECTION_STR)"; \
	kubectl create secret generic dxpy-api-secret --from-literal=DX_API_TOKEN=$(DX_API_TOKEN); \

install_azurite:
	kubectl apply -f k8s/azure_emulator/azurite-pvc.yaml; \
	kubectl apply -f k8s/azure_emulator/azurite-service.yaml; \
	kubectl apply -f k8s/azure_emulator/azurite-deployment.yaml; \
	echo "Waiting 10 seconds for azurite to start..."; \
	sleep 10; \
	kubectl delete job azurite-init --ignore-not-found && kubectl apply -f k8s/azure_emulator/azurite-init-containers.yaml

ACR_NAME ?= raydemo
TENANT_ID ?= $$TENANT_ID
link_acr:
	az login --tenant $(TENANT_ID); \
	sudo az acr login --name $(ACR_NAME); \
	ACR_LOGIN_SERVER=$$(az acr show --name $(ACR_NAME) --query loginServer -o tsv); \
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

linux_one_shot: init_linux install_k8s install_ray install_secrets link_acr 
