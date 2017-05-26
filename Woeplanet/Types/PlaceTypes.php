<?php

namespace Woeplanet\Types;

class PlaceTypes {
    private $places = [
        0 => [
            'id' => 0,
            'name' => 'Unknown',
            'description' => 'Unknown, empty or invalid place type',
            'shortname' => 'Unknown',
            'tag' => 'unknown'
        ],
        6 => [
            'id' => 6,
            'name' => 'Street',
            'description' => 'A street',
            'shortname' => 'Street',
            'tag' => 'street'
        ],
        7 => [
            'id' => 7,
            'name' => 'Town',
            'description' => 'A populated settlement such as a city, town, village',
            'shortname' => 'Town',
            'tag' => 'town'
        ],
        8 => [
            'id' => 8,
            'name' => 'State',
            'description' => 'One of the primary administrative areas within a country',
            'shortname' => 'State',
            'tag' => 'state'
        ],
        9 => [
            'id' => 9,
            'name' => 'County',
            'description' => 'One of the secondary administrative areas within a country',
            'shortname' => 'County',
            'tag' => 'county'
        ],
        10 => [
            'id' => 10,
            'name' => 'Local Administrative Area',
            'description' => 'One of the tertiary administrative areas within a country',
            'shortname' => 'LocalAdmin',
            'tag' => 'local-admin'
        ],
        11 => [
            'id' => 11,
            'name' => 'Postal Code',
            'description' => 'A partial or full postal code',
            'shortname' => 'Zip',
            'tag' => 'zip'
        ],
        12 => [
            'id' => 12,
            'name' => 'Country',
            'description' => 'One of the countries or dependent territories defined by the ISO 3166-1 standard',
            'shortname' => 'Country',
            'tag' => 'country'
        ],
        13 => [
            'id' => 13,
            'name' => 'Island',
            'description' => 'An island',
            'shortname' => 'Island',
            'tag' => 'island'
        ],
        14 => [
            'id' => 14,
            'name' => 'Airport',
            'description' => 'An airport',
            'shortname' => 'Airport',
            'tag' => 'airport'
        ],
        15 => [
            'id' => 15,
            'name' => 'Drainage',
            'description' => 'A water feature such as a river, canal, lake, bay, ocean',
            'shortname' => 'Drainage',
            'tag' => 'drainage'
        ],
        16 => [
            'id' => 16,
            'name' => 'Land Feature',
            'description' => 'A land feature such as a park, mountain, beach',
            'shortname' => 'LandFeature',
            'tag' => 'land-feature'
        ],
        17 => [
            'id' => 17,
            'name' => 'Miscellaneous',
            'description' => 'A uncategorized place',
            'shortname' => 'Miscellaneous',
            'tag' => 'misc'
        ],
        18 => [
            'id' => 18,
            'name' => 'Nationality',
            'description' => 'An area affiliated with a nationality',
            'shortname' => 'Nationality',
            'tag' => 'nationality'
        ],
        19  => [
            'id' => 19,
            'name' => 'Supername',
            'description' => 'An area covering multiple countries',
            'shortname' => 'Supername',
            'tag' => 'supername'
        ],
        20 => [
            'id' => 20,
            'name' => 'Point of Interest',
            'description' => 'A point of interest such as a school, hospital, tourist attraction',
            'shortname' => 'POI',
            'tag' => 'poi'
        ],
        21 => [
            'id' => 21,
            'name' => 'Region',
            'description' => 'An area covering portions of several countries',
            'shortname' => 'Region',
            'tag' => 'region'
        ],
        22 => [
            'id' => 22,
            'name' => 'Suburb',
            'description' => 'A subdivision of a town such as a suburb or neighborhood',
            'shortname' => 'Suburb',
            'tag' => 'suburb'
        ],
        23 => [
            'id' => 23,
            'name' => 'Sports Team',
            'description' => 'A sports team',
            'shortname' => 'Sport',
            'tag' => 'sports-team'
        ],
        24 => [
            'id' => 24,
            'name' => 'Colloquial',
            'description' => 'A place known by a colloquial name',
            'shortname' => 'Colloquial',
            'tag' => 'colloquial'
        ],
        25 => [
            'id' => 25,
            'name' => 'Zone',
            'description' => 'An area known within a specific context such as MSA or area code',
            'shortname' => 'Zone',
            'tag' => 'zone'
        ],
        26 => [
            'id' => 26,
            'name' => 'Historical State',
            'description' => 'A historical primary administrative area within a country',
            'shortname' => 'HistoricalState',
            'tag' => 'historical-state'
        ],
        27 => [
            'id' => 27,
            'name' => 'Historical County',
            'description' => 'A historical secondary administrative area within a country',
            'shortname' => 'HistoricalCounty',
            'tag' => 'historical-county'
        ],
        29 => [
            'id' => 29,
            'name' => 'Continent',
            'description' => 'One of the major land masses on the Earth',
            'shortname' => 'Continent',
            'tag' => 'continent'
        ],
        31 => [
            'id' => 31,
            'name' => 'Time Zone',
            'description' => 'An area defined by the Olson standard (tz database)',
            'shortname' => 'Timezone',
            'tag' => 'timezone'
        ],
        32 => [
            'id' => 32,
            'name' => 'Nearby Intersection',
            'description' => 'An intersection of streets that is nearby to the streets in a query string',
            'shortname' => 'Nearby Intersection',
            'tag' => 'nearby-intersection'
        ],
        33 => [
            'id' => 33,
            'name' => 'Estate',
            'description' => 'A housing development or subdivision known by name',
            'shortname' => 'Estate',
            'tag' => 'estate'
        ],
        35 => [
            'id' => 35,
            'name' => 'Historical Town',
            'description' => 'A historical populated settlement that is no longer known by its original name',
            'shortname' => 'HistoricalTown',
            'tag' => 'historical-town'
        ],
        36 => [
            'id' => 36,
            'name' => 'Aggregate',
            'description' => 'An aggregate place',
            'shortname' => 'Aggregate',
            'tag' => 'aggregate'
        ],
        37 => [
            'id' => 37,
            'name' => 'Ocean',
            'description' => 'One of the five major bodies of water on the Earth',
            'shortname' => 'Ocean',
            'tag' => 'ocean'
        ],
        38 => [
            'id' => 38,
            'name' => 'Sea',
            'description' => 'An area of open water smaller than an ocean',
            'shortname' => 'Sea',
            'tag' => 'sea'
        ]
    ];

    private $types_by_shortname;

    public function __construct() {
        $this->types_by_shortname = [];
        $this->types_by_tag = [];

        foreach ($this->places as $id => $pt){
            $this->types_by_shortname[$pt['shortname']] = $pt;
            $this->types_by_tag[$pt['tag']] = $pt;
        }
    }

    public function get() {
        return $this->places;
    }

    public function get_by_shortname($shortname) {
        if (array_key_exists($shortname, $this->types_by_shortname)) {
            return [
                'found' => true,
                'placetype' => $this->types_by_shortname[$shortname]
            ];
        }
        return [
            'found' => false,
            'placetype' => []
        ];
    }

    public function get_by_id($id) {
        if (array_key_exists($id, $this->places)) {
            return [
                'found' => true,
                'placetype' => $this->places[$id]
            ];
        }

        else {
            return [
                'found' => false,
                'placetype' => []
            ];
        }
    }

    public function get_by_tag($tag) {
        if (array_key_exists($tag, $this->types_by_tag)) {
            return [
                'found' => true,
                'placetype' => $this->types_by_tag[$tag]
            ];
        }
        return [
            'found' => false,
            'placetype' => []
        ];
    }
}

?>
