FROM rayproject/ray:2.46.0

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY ./ .

RUN pwd
RUN ls
