FROM rayproject/ray:2.46.0

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY native_python/ray_integration/ray_downloader.py native_python/ray_integration/

RUN pwd
RUN ls
