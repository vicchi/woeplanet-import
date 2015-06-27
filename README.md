woeplanet-import
================

This repository contains code to import and index GeoPlanet (AKA WOE,
also AKA WOEIDs) into Elasticsearch. Some things you should know about this.

* This repository doesn't contain the actual raw import data from the
Yahoo! GeoPlanet Data dump files. They used to live [here](https://developer.yahoo.com/geo/geoplanet/data/)
but that site has been saying *We are currently making the data non-downloadable while we determine a better way to surface the data as a part of the service* for
too long now. Thank to [Aaron](https://twitter.com/thisisaaronland) they've been
mirrored on the [Internet Archive](https://archive.org/search.php?query=geoplanet) which
is just the sort of thing the Archive was designed for.

* This repository doesn't contain an Elasticsearch index or the output of an
Elasticsearch index in GeoJSON format. If you want the former, you'll need to
run the import scripts in this repository. If you want the latter, take a
look [here](https://github.com/vicchi/woeplanet-data).

Import GeoPlanet into Elasticsearch

7.3.1 - 26 mins, 1.6 GB cache
7.3.2 - 45 mins, 2.8 GB cache
7.4.0 - 1 hour, 4 GB cache
7.4.1 - 1 hour 14 mins, 5.3 GB cache
7.5.1 - 49 mins, 5.3 GB cache
7.5.2 - 1 hour, 5.3 GB cache
7.6.0 - 1 hour, 5.3 GB cache
7.8.1 - 2 hours 40 mins, 5.3 GB cache
7.9.0 - 1 hour 42 mins, 5.5 GB cache
7.10.0 - 2 hours 18 mins, 6.3 GB cache
8.0 - 3 hours 7 mins, 7 GB cache

Extract Quattroshapes Gazetteer

$ wget http://static.quattroshapes.com/quattroshapes_gazetteer_gp_then_gn.zip
$ unzip quattroshapes_gazetteer_gp_then_gn.zip
$ ogr2ogr -select woe_id,gn_id,qs_id,gn_name,language -f CSV shp/quattroshapes_gazetteer_gp_then_gn.csv shp/quattroshapes_gazetteer_gp_then_gn.shp -lco GEOMETRY=AS_XY
