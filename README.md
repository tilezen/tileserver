# tileserver

A lightweight tileserver to share code paths with tilequeue for tile generation.

## Installation

First, install some dependencies:

* `integration-1` branch of the `mapzen/TileStache` fork
* latest tilequeue `master` branch

Then:

    python setup.py develop

## Usage

At the moment, tileserver assumes that the `mapzen/vector-datasource` is checked out as a sibling. This will change to a configuration file shortly.

    python tileserver/__init__.py
