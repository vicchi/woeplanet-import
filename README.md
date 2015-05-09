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
