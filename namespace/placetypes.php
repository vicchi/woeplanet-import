<?php

namespace Woeplanet;

class PlaceTypes {
    private $placetypes = array(
        0 => array(
            'id' => 0,
            'name' => 'Unknown',
            'description' => 'Unknown, empty or invalid place type',
            'shortname' => 'Unknown',
            'tag' => 'unknown'
        ),
        6 => array(
            'id' => 6,
            'name' => 'Street',
            'description' => 'A street',
            'shortname' => 'Street',
            'tag' => 'street'
        ),
        7 => array (
            'id' => 7,
            'name' => 'Town',
            'description' => 'A populated settlement such as a city, town, village',
            'shortname' => 'Town',
            'tag' => 'town'
        ),
        8 => array (
            'id' => 8,
            'name' => 'State',
            'description' => 'One of the primary administrative areas within a country',
            'shortname' => 'State',
            'tag' => 'state'
        ),
        9 => array (
            'id' => 9,
            'name' => 'County',
            'description' => 'One of the secondary administrative areas within a country',
            'shortname' => 'County',
            'tag' => 'county'
        ),
        10 => array (
            'id' => 10,
            'name' => 'Local Administrative Area',
            'description' => 'One of the tertiary administrative areas within a country',
            'shortname' => 'LocalAdmin',
            'tag' => 'local-admin'
        ),
        11 => array (
            'id' => 11,
            'name' => 'Postal Code',
            'description' => 'A partial or full postal code',
            'shortname' => 'Zip',
            'tag' => 'zip'
        ),
        12 => array (
            'id' => 12,
            'name' => 'Country',
            'description' => 'One of the countries or dependent territories defined by the ISO 3166-1 standard',
            'shortname' => 'Country',
            'tag' => 'country'
        ),
        13 => array (
            'id' => 13,
            'name' => 'Island',
            'description' => 'An island',
            'shortname' => 'Island',
            'tag' => 'island'
        ),
        14 => array(
            'id' => 14,
            'name' => 'Airport',
            'description' => 'An airport',
            'shortname' => 'Airport',
            'tag' => 'airport'
        ),
        15 => array (
            'id' => 15,
            'name' => 'Drainage',
            'description' => 'A water feature such as a river, canal, lake, bay, ocean',
            'shortname' => 'Drainage',
            'tag' => 'drainage'
        ),
        16 => array (
            'id' => 16,
            'name' => 'Land Feature',
            'description' => 'A land feature such as a park, mountain, beach',
            'shortname' => 'LandFeature',
            'tag' => 'land-feature'
        ),
        17 => array (
            'id' => 17,
            'name' => 'Miscellaneous',
            'description' => 'A uncategorized place',
            'shortname' => 'Miscellaneous',
            'tag' => 'misc'
        ),
        18 => array (
            'id' => 18,
            'name' => 'Nationality',
            'description' => 'An area affiliated with a nationality',
            'shortname' => 'Nationality',
            'tag' => 'nationality'
        ),
        19 =>array (
            'id' => 19,
            'name' => 'Supername',
            'description' => 'An area covering multiple countries',
            'shortname' => 'Supername',
            'tag' => 'supername'
        ),
        20 => array (
            'id' => 20,
            'name' => 'Point of Interest',
            'description' => 'A point of interest such as a school, hospital, tourist attraction',
            'shortname' => 'POI',
            'tag' => 'poi'
        ),
        21 => array (
            'id' => 21,
            'name' => 'Region',
            'description' => 'An area covering portions of several countries',
            'shortname' => 'Region',
            'tag' => 'region'
        ),
        22 => array (
            'id' => 22,
            'name' => 'Suburb',
            'description' => 'A subdivision of a town such as a suburb or neighborhood',
            'shortname' => 'Suburb',
            'tag' => 'suburb'
        ),
        23 => array (
            'id' => 23,
            'name' => 'Sports Team',
            'description' => 'A sports team',
            'shortname' => 'Sport',
            'tag' => 'sports-team'
        ),
        24 => array (
            'id' => 24,
            'name' => 'Colloquial',
            'description' => 'A place known by a colloquial name',
            'shortname' => 'Colloquial',
            'tag' => 'colloquial'
        ),
        25 => array (
            'id' => 25,
            'name' => 'Zone',
            'description' => 'An area known within a specific context such as MSA or area code',
            'shortname' => 'Zone',
            'tag' => 'zone'
        ),
        26 => array (
            'id' => 26,
            'name' => 'Historical State',
            'description' => 'A historical primary administrative area within a country',
            'shortname' => 'HistoricalState',
            'tag' => 'historical-state'
        ),
        27 => array (
            'id' => 27,
            'name' => 'Historical County',
            'description' => 'A historical secondary administrative area within a country',
            'shortname' => 'HistoricalCounty',
            'tag' => 'historical-county'
        ),
        29 => array (
            'id' => 29,
            'name' => 'Continent',
            'description' => 'One of the major land masses on the Earth',
            'shortname' => 'Continent',
            'tag' => 'continent'
        ),
        31 => array (
            'id' => 31,
            'name' => 'Time Zone',
            'description' => 'An area defined by the Olson standard (tz database)',
            'shortname' => 'Timezone',
            'tag' => 'timezone'
        ),
        32 => array (
            'id' => 32,
            'name' => 'Nearby Intersection',
            'description' => 'An intersection of streets that is nearby to the streets in a query string',
            'shortname' => 'Nearby Intersection',
            'tag' => 'nearby-intersection'
        ),
        33 => array (
            'id' => 33,
            'name' => 'Estate',
            'description' => 'A housing development or subdivision known by name',
            'shortname' => 'Estate',
            'tag' => 'estate'
        ),
        35 => array (
            'id' => 35,
            'name' => 'Historical Town',
            'description' => 'A historical populated settlement that is no longer known by its original name',
            'shortname' => 'HistoricalTown',
            'tag' => 'historical-town'
        ),
        36 => array (
            'id' => 36,
            'name' => 'Aggregate',
            'description' => 'An aggregate place',
            'shortname' => 'Aggregate',
            'tag' => 'aggregate'
        ),
        37 => array (
            'id' => 37,
            'name' => 'Ocean',
            'description' => 'One of the five major bodies of water on the Earth',
            'shortname' => 'Ocean',
            'tag' => 'ocean'
        ),
        38 => array (
            'id' => 38,
            'name' => 'Sea',
            'description' => 'An area of open water smaller than an ocean',
            'shortname' => 'Sea',
            'tag' => 'sea'
        )
    );

    private $types_by_shortname;

    public function __construct() {
        $this->types_by_shortname = array();

        foreach ($this->placetypes as $id => $pt){
            $this->types_by_shortname[$pt['shortname']] = $pt;
        }
    }

    public function get() {
        return $this->placetypes;
    }

    public function get_by_shortname($shortname) {
        if (array_key_exists($shortname, $this->types_by_shortname)) {
            return array(
                'found' => true,
                'placetype' => $this->types_by_shortname[$shortname]
            );
        }
        return array(
            'found' => false,
            'placetype' => array()
        );
    }

    public function get_by_id($id) {
        if (array_key_exists($id, $this->placetypes)) {
            return array(
                'found' => true,
                'placetype' => $this->placetypes[$id]
            );
        }

        else {
            return array(
                'found' => false,
                'placetype' => array()
            );
        }
    }
}

?>
