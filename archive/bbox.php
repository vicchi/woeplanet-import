#!/usr/bin/env php
<?php

$swlon = 138.511032;
$swlat = -34.958771;
$nelon = 138.546494;
$nelat = -34.931835;

$bbox = new BoundingBox($swlon, $swlat, $nelon, $nelat);
$geojson = $bbox->to_geojson();
var_dump($geojson);
var_dump(json_encode($geojson));

$envelope = $bbox->to_envelope();
var_dump($envelope);
var_dump(json_encode($envelope));

class BoundingBox {
    private $swlon;
    private $swlat;
    private $nelon;
    private $nelat;

    public function __construct($swlon, $swlat, $nelon, $nelat) {
        $this->swlon = $swlon;
        $this->swlat = $swlat;
        $this->nelon = $nelon;
        $this->nelat = $nelat;
    }

    public function to_geojson() {
        return array($this->swlon, $this->swlat, $this->nelon, $this->nelat);
    }

    public function to_envelope() {
        $north = $this->nelat;
        $east = $this->nelon;
        $south = $this->swlat;
        $west = $this->swlon;

        return array(
            array($east, $south),
            array($west, $north)
        );
    }
}

?>
