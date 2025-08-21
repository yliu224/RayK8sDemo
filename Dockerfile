FROM rayproject/ray:2.49.0.66438d-py310-aarch64

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN python --version

COPY demo/ demo/
COPY main.py main.py

