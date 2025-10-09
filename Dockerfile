FROM rayproject/ray:nightly-py310-cu128

# This is for local testing only. It's installing the Azure CLI.
RUN sudo apt-get update && sudo apt-get install -y curl && curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash;

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN python --version

COPY demo/ demo/
COPY main.py main.py


