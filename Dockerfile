FROM python:2

RUN apt-get -y update \
 && apt-get -y install \
                libgeos-dev \
 && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
RUN mkdir -p /usr/src/vector-datasource \
 && git clone --depth 1 https://github.com/mapzen/vector-datasource.git /usr/src/vector-datasource

COPY . /usr/src/app
RUN pip install --no-cache-dir -r requirements.txt \
 && pip install -e . \
 && pip install -e ../vector-datasource \
 && pip install -U gunicorn

CMD [ "gunicorn", "--bind", "0.0.0.0", "--timeout", "90", "--workers", "5", "--reload", "--access-logfile", "-", "tileserver:wsgi_server('config.docker-compose.yaml')"]
