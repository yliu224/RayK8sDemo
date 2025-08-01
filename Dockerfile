FROM rayproject/ray:2.46.0

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY counter.py .
COPY main.py .

RUN pwd
RUN ls
