# tileserver

A lightweight tileserver to share code paths with tilequeue for tile generation.

## Installation

First, install some dependencies:

* `integration-1` branch of the `mapzen/TileStache` fork
* latest tilequeue `master` branch

Then:

    python setup.py develop

## Usage

    cp config.yaml.sample config.yaml
    python tileserver/__init__.py config.yaml
