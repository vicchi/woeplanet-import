<?php

namespace Woeplanet\Types;

class Centroid {
    private $lon;
    private $lat;

    public function __construct($lon, $lat, $validate=false) {
        $this->lon = (double)$lon;
        $this->lat = (double)$lat;

        if ($validate) {
            if ($this->lon < -180.0 || $this->lon > 180.0) {
                throw new \Exception('Longitude of ' . $this->lon . ' is out of bounds');
            }
            if ($this->lat < -90.0 || $this->lat > 90.0) {
                throw new \Exception('Latitude of ' . $this->lat . ' is out of bounds');
            }
        }
    }

    public function lon() {
        return $this->lon;
    }

    public function lat() {
        return $this->lat;
    }

    public function coordinates() {
        return [$this->lon, $this->lat];
    }

    public function is_empty() {
        return ($this->lon == 0.0 && $this->lat == 0.0);
    }

    public function is_valid() {
        if ($this->lon < -180.0 || $this->lon > 180.0) {
            return false;
        }
        if ($this->lat < -90.0 || $this->lat > 90.0) {
            return false;
        }

        return true;
    }

    public function to_geojson() {
        return [
            'type' => 'Point',
            'coordinates' => [$this->lon, $this->lat]
        ];
    }

    public function to_json() {
        return [$this->lon, $this->lat];
    }

    public function to_wkt() {
        return "POINT($this->lon $this->lat)";
    }

    public function __toString() {
        return 'Centroid(' . $this->lon . ',' . $this->lat . ')';
    }
}

?>
