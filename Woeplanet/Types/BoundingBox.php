<?php

namespace Woeplanet\Types;

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

    public function coordinates() {
        return [$this->swlon, $this->swlat, $this->nelon, $this->nelat];
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

		return [
			[$east, $south],
			[$west, $north]
		];
	}

	public function to_geojson() {
		$north = $this->nelat;
		$east = $this->nelon;
		$south = $this->swlat;
		$west = $this->swlon;

		return [
			'type' => 'Polygon',
			'coordinates' => [
				[
					[$this->swlon, $this->swlat],
					[$west, $north],
					[$this->nelon, $this->nelat],
					[$east, $south],
					[$this->swlon, $this->swlat]
				]
			]
		];
	}

    public function to_json() {
		$north = $this->nelat;
		$east = $this->nelon;
		$south = $this->swlat;
		$west = $this->swlon;

		return [
			'type' => 'polygon',
			'coordinates' => [
				[
					[$this->swlon, $this->swlat],
					[$west, $north],
					[$this->nelon, $this->nelat],
					[$east, $south],
					[$this->swlon, $this->swlat]
				]
			]
		];
	}

    public function to_wkt() {
        $north = $this->nelat;
        $east = $this->nelon;
        $south = $this->swlat;
        $west = $this->swlon;

        return "POLYGON(($this->swlon $this->swlat, $west $north, $this->nelon $this->nelat, $east $south, $this->swlon $this->swlat))";
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
