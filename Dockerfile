FROM rayproject/ray:nightly-py310-cu128

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt
RUN python --version

COPY demo/ demo/
COPY main.py main.py

