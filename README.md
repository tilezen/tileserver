# tileserver

A lightweight tileserver to share code paths with tilequeue for tile generation.

## Installation

We recommend following the vector-datasource [installation instructions](https://github.com/tilezen/vector-datasource/wiki/Mapzen-Vector-Tile-Service).

There is a requirements file that can be used to install.

    pip install -Ur requirements.txt

Then:

    python setup.py develop

## Usage

    cp config.yaml.sample config.yaml
    python tileserver/__init__.py config.yaml
