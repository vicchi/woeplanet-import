<?php

namespace WoePlanet\GeoUtils;

class Point {
    private $lat;
    private $lon;

    public function __construct($lat, $lon) {
        $this->lat = $lat;
        $this->lon = $lon;
    }

    public function lat() {
        return $this->lat;
    }

    public function lon() {
        return $this->lon;
    }

    public function geojson($assoc=false) {
        $geojson = array($this->lon,$this->lat);

        if ($assoc) {
            return $geojson;
        }

        return json_encode($geojson);
    }
}

class BoundingBox {
    private $sw;
    private $ne;

    public function __construct($sw, $ne) {
        $this->sw = $sw;
        $this->ne = $ne;
    }


    public function geojson($assoc=false) {
    $geojson = array(
    'bbox' => array(
    $this->sw->geojson(true),
    $this->ne->geojson(true)
    )
    );

        if ($assoc) {
            return $geojson;
        }

        return json_encode($geojson);
     }

    // ne == upper right
    // sw == lower left

    public function sw() {
        return $this->sw;
    }

    public function ne() {
        return $this->ne;
    }

    // se == lower right
    // nw == upper left

    public function se() {
        return new Point($this->east, $this->south);
    }

    public function nw() {
        return new Point($this->west, $this->north);
    }

    public function lower_right() {
        return $this->se();
    }

    public function upper_left() {
        return $this->nw();
    }

    private function north() {
        return $this->ne->lat();
    }

    private function east() {
        return $this->ne->lon();
    }

    private function south() {
        return $this->sw->lat();
    }

    private function west() {
        return $this->sw->lon();
    }
}

?>
