<?php

namespace Woeplanet;

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
        return array(
            'type' => 'Point',
            'coordinates' => array($this->lon, $this->lat)
        );
    }

    public function __toString() {
        return 'Centroid(' . $this->lon . ',' . $this->lat . ')';
    }
}

class BoundingBox {
	private $swlon;
	private $swlat;
	private $nelon;
	private $nelat;

	public function __construct($swlon, $swlat, $nelon, $nelat, $validate=false) {
		$this->swlon = (double) $swlon;
		$this->swlat = (double) $swlat;
		$this->nelon = (double) $nelon;
		$this->nelat = (double) $nelat;

        if ($validate) {
            if ($this->swlon == $this->nelon && $this->swlat == $this->nelat) {
                throw new \Exception('Bounding box SW and NE corners are equal');
            }
        }
	}

	public function is_empty() {
		return ($this->swlon == 0 && $this->swlat == 0 && $this->nelon == 0 && $this->nelat == 0);
	}

    public function is_valid() {
        if ($this->swlon == $this->nelon && $this->swlat == $this->nelat) {
            return false;
        }

        if ($this->swlon == $this->nelon || $this->swlat == $this->nelat) {
            return false;
        }

        return true;
    }

	public function to_bbox() {
		$north = $this->nelat;
		$east = $this->nelon;
		$south = $this->swlat;
		$west = $this->swlon;

		return array(
			array($east, $south),
			array($west, $north)
		);
	}

	public function to_geojson() {
		$north = $this->nelat;
		$east = $this->nelon;
		$south = $this->swlat;
		$west = $this->swlon;

		return array(
			'type' => 'Polygon',
			'coordinates' => array(
				array(
					array($this->swlon, $this->swlat),
					array($west, $north),
					array($this->nelon, $this->nelat),
					array($east, $south),
					array($this->swlon, $this->swlat)
				)
			)
		);
	}

    public function __toString() {
        $north = $this->nelat;
        $east = $this->nelon;
        $south = $this->swlat;
        $west = $this->swlon;

        return 'BBox(' .
            '[' . $east . ',' . $south .  ']' .
            '[' . $west . ',' . $north . ']' .
        ')';
    }

}

?>
