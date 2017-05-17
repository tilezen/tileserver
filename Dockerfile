FROM python:2

RUN apt-get -y update \
 && apt-get -y install libgeos-dev \
                       libpq-dev \
                       python-pil \
                       libmapnik2.2 \
                       libmapnik-dev \
                       mapnik-utils \
                       python-mapnik \
 && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY . /usr/src/app
RUN mkdir -p /usr/src/vector-datasource \
 && git clone --depth 1 https://github.com/mapzen/vector-datasource.git /usr/src/vector-datasource

RUN pip install --no-cache-dir -r requirements.txt \
 && pip install -e . \
 && pip install -e ../vector-datasource \
 && pip install -U gunicorn

CMD [ "gunicorn", "--bind", "0.0.0.0", "tileserver:wsgi_server('config.yaml')"]
