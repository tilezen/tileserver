# tileserver

A lightweight tileserver to share code paths with tilequeue for tile generation.

## Installation

We recommend following the vector-datasource [installation instructions](https://github.com/tilezen/vector-datasource/wiki/Mapzen-Vector-Tile-Service).

There is a requirements file that can be used to install.

    pip install -Ur requirements.txt

Then:

    python setup.py develop

## Installation (detailed)

In addition to the dependencies in [requirements.txt](requirements.txt), tileserver requires

* PostgreSQL client-side development library and headers (for psycopg)
* GEOS library

These can be installed on Debian-based systems with
```
sudo apt-get install libpq-dev libgeos-c1v5
```

Then install the python requirements with

    pip install -Ur requirements.txt

Then:

    python setup.py develop

## Usage

    cp config.yaml.sample config.yaml
    python tileserver/__init__.py config.yaml
