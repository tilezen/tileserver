# Running Tileserver with Docker

Tileserver is part of [the larger Tilezen ecosystem](https://github.com/tilezen/vector-datasource/wiki/Mapzen-Vector-Tile-Service) that Mapzen uses to serve its [Mapzen Vector Tiles service](https://mapzen.com/projects/vector-tiles/). If you'd like to get started quickly or try your hand at submitting a change to the Tilezen stack, you can run Tileserver as a standalone application using Docker to skip most of the dependency and installation steps. Follow along below to get tiles in 15 minutes!

**Note**: This setup is recommended for local, single-person use as a demo only. We don't recommend using this setup for production or development work of vector tiles. Tileserver does not have strong support for caching or data updates from OpenStreetMap. To support a production environment or data updates from OpenStreetMap, please [follow our complete Tilezen setup instructions](https://github.com/tilezen/vector-datasource/wiki/Mapzen-Vector-Tile-Service).

### Install Docker

This walkthrough assumes you already have Docker installed on your computer. Docker's website [has installation instructions](https://docs.docker.com/engine/installation/) for most common environments.

### Setup Tileserver environment

Once you have Docker installed, you can use [`docker-compose`](https://docs.docker.com/compose/) and the included `docker-compose.yaml` file to get a complete system ready:

1. If you haven't already, check out the `tileserver` repository from GitHub:

   ```
   git clone https://github.com/tilezen/tileserver.git
   ```

1. Run the `docker-compose` command from inside the tileserver folder you just checked out:

   ```
   cd tileserver
   docker-compose up
   ```

   This will write out a lot of messages, but at the end you should see `postgis_1` telling you "database system is ready to accept connections":

   ```
   postgis_1     | LOG:  MultiXact member wraparound protections are now enabled
   postgis_1     | LOG:  database system is ready to accept connections
   ```

   **Congratulations**, you have a working tileserver, database, and caching system running. It's empty and ready to import map data.

### Load map data

Your tileserver system is running but does not contain any map data. Let's give it something to work on!

Let's pick a metropolitan area to load into your database. Loading the entire planet's worth of data is beyond the scope of this exercise - it takes up too much space and time!

1. Head to Mapzen's [Metro Extracts service](https://mapzen.com/data/metro-extracts/) and select a "popular extract" you're interested in.

   For example, I picked [Minneapolis/Saint Paul](https://mapzen.com/data/metro-extracts/metro/minneapolis-saint-paul_minnesota/). Pull out the last bit of that URL (the `minneapolis-saint-paul_minnesota` part) to get the "extract name" that we'll use later.

1. The `docker-compose up` command should still be running. In another shell window, run the following command to start data loading:

   ```
   docker run \
    --link tileserver_postgis_1:postgres \
    --net tileserver_default \
    -e METRO_EXTRACT_NAME=minneapolis-saint-paul_minnesota \
    -it --rm --name vector-datasource-loader \
    mapzen/vector-datasource
   ```

   This will write out lots of log messages and take several minutes depending on how big of a metro area you chose. At the end you'll see "All updates complete. Exiting." and some final cleanup steps and it will be ready.

### Explore the map

After the data load is complete, you can head to [`http://localhost:8000/preview.html`](http://localhost:8000/preview.html) and view a map with your vector tiles. The map might not be centered on the data extract you loaded, so you'll probably need to pan your way to wherever the data is.
