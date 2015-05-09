#!/usr/local/bin/php -q
<?php

/*

	-v | --verbose
	-e | --elasticsearch
	-p | --purge
	-g | --geoplanet
	-s | --stage
 */

require 'vendor/autoload.php';

$instance = "http://localhost:9200";
$verbose = true;
$geoplanet = "/Users/gary/Projects/GPLplanet/import/data/geoplanet_data_7.10.0";

$options = get_options();

const PLACES_WOEID = 0;
const PLACES_ISO = 1;
const PLACES_NAME = 2;
const PLACES_LANG = 3;
const PLACES_TYPE = 4;
const PLACES_PARENT = 5;

$match = array();
$pattern = '/geoplanet_data_([\d\.]+)$/';
$ret = preg_match($pattern, basename($geoplanet), $match);
$version = $match[1];

$places = sprintf('geoplanet_places_%s.tsv', $version);
$aliases = sprintf('geoplanet_aliases_%s.tsv', $version);
$adjacencies = sprintf('geoplanet_adjacencies_%s.tsv', $version);
$changes = sprintf('geoplanet_changes_%s.tsv', $version);
$coords = sprintf('geoplanet_coords_%s.tsv', $version);
$admins = sprintf('geoplanet_admins_%s.tsv', $version);

$files = array();

$dir = opendir($options['geoplanet']);
while (false !== ($entry = readdir($dir))) {
	if ($entry === '.' || $entry === '..')
		continue;

	$files[$entry] = $geoplanet . DIRECTORY_SEPARATOR . $entry;
}

if (false === array_key_exists($places, $files)) {
	echo "Missing " . $places . "\n";
	exit;
}

$es = new Elasticsearch\Client();

$sqlite = new PDO('sqlite:geoplanet.sqlite3');

if ($options['stage'] == 'all' || $options['stage'] == 'coords') {
	echo $files[$coords] . "\n";
	parse_coords($options, $files[$coords], $sqlite);
	$options['stage'] = 'all';
}

if ($options['stage'] == 'all' || $options['stage'] == 'places') {
	echo $files[$places] . "\n";
	parse_places($options, $files[$places], $es, $sqlite);
	$options['stage'] = 'all';
}

if ($options['stage'] == 'all' || $options['stage'] == 'adjacencies') {
	echo $files[$adjacencies] . "\n";
	parse_adjacencies($options, $files[$adjacencies], $es, $sqlite);
	$options['stage'] = 'all';
}

if ($options['stage'] == 'all' || $options['stage'] == 'admins') {
	parse_admins($options, $files[$admins], $es);
	$options['stage'] = 'all';
}

if ($options['stage'] == 'all' || $options['stage'] == 'placetypes') {
	index_placetypes($options, $es);
	$options['stage'] = 'all';
}

if ($options['stage'] == 'all' || $options['stage'] == 'aliases') {
	parse_aliases($options, $files[$aliases], $es, $sqlite);
	$options['stage'] = 'all';
}


function parse_coords($options, $coords, $db) {
	echo "Loading and cacheing coordinates\n";

	$total = total_entries($coords);

	if ($options['purge']) {
		$drop = "DROP TABLE geoplanet_coords";
		$db->exec($drop);
	}

	$setup = "CREATE TABLE geoplanet_coords(
		woeid INTEGER PRIMARY KEY,
		lat REAL,
		lon REAL,
		swlat REAL,
		swlon REAL,
		nelat REAL,
		nelon REAL);";
	$db->exec($setup);

	$tsv = new TSVReader($coords);
	$row = 0;

	while (($data = $tsv->get()) !== false) {
		$row++;

		$insert = "INSERT INTO geoplanet_coords(woeid,lat,lon,swlat,swlon,nelat,nelon)
			VALUES(:woeid,:lat,:lon,:swlat,:swlon,:nelat,:nelon)";

		$statement = $db->prepare($insert);
		$statement->bindParam(':woeid', $data['WOE_ID']);
		$statement->bindParam(':lat', $data['Lat']);
		$statement->bindParam(':lon', $data['Lon']);
		$statement->bindParam(':swlat', $data['SW_Lat']);
		$statement->bindParam(':swlon', $data['SW_Lon']);
		$statement->bindParam(':nelat', $data['NE_Lat']);
		$statement->bindParam(':nelon', $data['NE_Lon']);

		$statement->execute();

		show_status($row, $total);
	}

	$tsv->close();
}

function parse_places($options, $places, $es, $db) {
	echo "Indexing places\n";
	global $version;	// yuck, for now

	$total = total_entries($places);

	// TODO - don't drop the entire index, just drop the type
	if ($options['purge']) {
		$purge = array(
			'index' => 'woeplanet'
		);
		$es->indices()->delete($purge);
	}

	if (($file = fopen($places, "r")) !== false) {
		$row = 0;
		$select = "SELECT * FROM geoplanet_coords WHERE woeid = :woeid";

		$provider = sprintf('geoplanet %s', $version);

		while (($data = fgetcsv($file, 1000, "\t")) !== false) {
			$row++;

			$params = array(
				'body' => array(
					'woeid' => $data[PLACES_WOEID],
					'iso' => $data[PLACES_ISO],
					'name' => $data[PLACES_NAME],
					'lang' => $data[PLACES_LANG],
					'type' => $data[PLACES_TYPE],
					'parent' => $data[PLACES_PARENT],
					'provider_metadata' => $provider
				),
				'index' => 'woeplanet',
				'type' => 'places',
				'id' => $data[PLACES_WOEID],
			);

			$statement = $db->prepare($select);
			$statement->bindParam(':woeid', $data[PLACES_WOEID]);
			$statement->execute();
			$coords = $statement->fetch(PDO::FETCH_ASSOC);
			$params['body']['centroid'] = array(
				'lat' => $coords['lat'],
				'lon' => $coords['lon']
			);
			$params['body']['bbox'] = array(
				'sw' => array(
					'lat' => $coords['swlat'],
					'lon' => $coords['swlon']
				),
				'ne' => array(
					'lat' => $coords['nelat'],
					'lon' => $coords['nelon']

				)
			);
			$es->index($params);
			show_status($row, $total);
		}
	}
}

function parse_adjacencies($options, $adjacencies, $es, $db) {
	global $version;	// yuck, for now
	$provider = sprintf('geoplanet %s', $version);

	$total = total_entries($adjacencies);

	if ($options['purge']) {
		echo 'Dropping table "geoplanet_adjacencies" from cache' . "\n";
		$drop = "DROP TABLE geoplanet_adjacencies";
		$db->exec($drop);
	}

	echo "Caching $total adjacencies\n";

	$setup = "CREATE TABLE geoplanet_adjacencies (
		woeid INTEGER,
		neighbour INTEGER
	)";
	$db->exec($setup);

	$setup = "CREATE INDEX adjacencies_by_woeid ON geoplanet_adjacencies (woeid)";
	$db->exec($setup);

	$tsv = new TSVReader($adjacencies);
	$row = 0;
	$insert = "INSERT INTO geoplanet_adjacencies(woeid, neighbour) VALUES(:woeid,:neighbour)";
	$statement = $db->prepare($insert);

	while (($data = $tsv->get()) !== false) {
		$row++;

		$statement->bindParam(':woeid', $data['Place_WOE_ID']);
		$statement->bindParam(':neighbour', $data['Neighbour_WOE_ID']);

		$statement->execute();

		show_status($row, $total);
	}

	echo "\nCached $row of $total adjacencies\n";

	$ids = array();

	$select = "SELECT COUNT(DISTINCT woeid) FROM geoplanet_adjacencies";
	$statement = $db->prepare($select);
	$statement->execute();
	$result = $statement->fetch(PDO::FETCH_ASSOC);
	$count = $result['COUNT(DISTINCT woeid)'];

	echo "Indexing $count adjacencies\n";

	$select = "SELECT DISTINCT(woeid) FROM geoplanet_adjacencies";
	//$select = "SELECT DISTINCT(woeid) FROM geoplanet_adjacencies WHERE woeid = 6";
	$distinct = $db->prepare($select);
	$distinct->execute();

	$select = "SELECT * FROM geoplanet_adjacencies WHERE woeid = :woeid";
	$statement = $db->prepare($select);
	$row = 0;

	while (($adj = $distinct->fetch(PDO::FETCH_ASSOC)) !== false) {
		$row++;
		$statement->bindParam(':woeid', $adj['woeid']);
		$statement->execute();

		$adjacent = array();

		while (($neighbour = $statement->fetch(PDO::FETCH_ASSOC)) !== false) {
			$adjacent[] = $neighbour['neighbour'];
		}

		$params = array(
			'body' => array(
				'doc' => array(
					'woeid' => $adj['woeid'],
					'woeid_adjacent' => $adjacent,
					'provider_metadata' => $provider
				),
				'doc_as_upsert' => true
			),
			'index' => 'woeplanet',
			'type' => 'places',
			'id' => $adj['woeid']
		);

		$es->update($params);
		show_status($row, $count);
	}

	echo "\nIndexed $row of $count adjacencies\n";
}

function parse_aliases($options, $aliases, $es, $db) {
	global $version;	// yuck, for now
	$provider = sprintf('geoplanet %s', $version);

	$total = total_entries($aliases);

	/*if ($options['purge']) {
		echo 'Dropping table "geoplanet_aliases" from cache' . "\n";
		$drop = "DROP TABLE geoplanet_aliases";
		$db->exec($drop);
	}

	echo "Caching $total aliases\n";

	$setup = "CREATE TABLE geoplanet_aliases (
		woeid INTEGER,
		name TEXT,
		type TEXT
	)";
	$db->exec($setup);

	$setup = "CREATE INDEX aliases_by_woeid ON geoplanet_aliases (woeid)";
	$db->exec($setup);

	$tsv = new TSVReader($aliases);
	$row = 0;
	$insert = "INSERT INTO geoplanet_aliases(woeid, name, type) VALUES(:woeid,:name,:type)";
	$statement = $db->prepare($insert);

	while (($data = $tsv->get()) !== false) {
		$row++;

		$statement->bindParam(':woeid', $data['WOE_ID']);
		$statement->bindParam(':name', $data['Name']);
		$statement->bindParam(':type', $data['Name_Type']);

		$statement->execute();

		show_status($row, $total);
	}

	echo "\nCached $row of $total aliases\n";*/

	$select = "SELECT COUNT(DISTINCT woeid) FROM geoplanet_aliases";
	$statement = $db->prepare($select);
	$statement->execute();
	$result = $statement->fetch(PDO::FETCH_ASSOC);
	$count = $result['COUNT(DISTINCT woeid)'];

	echo "Indexing $count aliases\n";

	$select = "SELECT DISTINCT(woeid) FROM geoplanet_aliases";
	$distinct = $db->prepare($select);
	$distinct->execute();

	$row = 0;
	$ids = array();
	echo "Aggregating $count candidate WOEIDs\n";
	while (($woeid = $distinct->fetch(PDO::FETCH_ASSOC)) !== false) {
		$row++;
		$ids[] = $woeid['woeid'];
		show_status($row, $count);
	}

	$total = count($ids);
	echo "Found $total candidate WOEIDs; indexing aliases\n";
	$select = "SELECT * FROM geoplanet_aliases WHERE woeid = :woeid";
	$statement = $db->prepare($select);

	$row = 0;
	foreach ($ids as $id) {
		//echo "starting woeid: $id\n";
		$row++;
		$statement->bindParam(':woeid', $id);
		$statement->execute();

		$aliases = array();
		while (($alias = $statement->fetch(PDO::FETCH_ASSOC)) !== false) {
			$woeid = $alias['woeid'];
			$name = $alias['name'];
			$type = $alias['type'];
			$key = sprintf('alias_%s', $type);
			//echo "woeid: $woeid, name: $name, type: $type\n";

			$names = array();
			if (array_key_exists($key, $aliases)) {
				$names = $aliases[$key];
			}
			$names[] = $name;
			$aliases[$key] = $names;
		}
		//show_status($row, $total);
		//echo "finished woeid: $id\n";
		//var_dump($aliases);
		$params = array(
			'body' => array(
				'doc' => array(
					'woeid' => $id,
					'provider_metadata' => $provider
				),
				'doc_as_upsert' => true
			),
			'index' => 'woeplanet',
			'type' => 'places',
			'id' => $id
		);

		foreach ($aliases as $key => $value) {
			$params['body']['doc'][$key] = $value;
		}

		//var_dump($params);
		$es->update($params);
		show_status($row, $total);
	}
}

function parse_admins($options, $admins, $es) {
	global $version;

	$provider = sprintf('geoplanet %s', $version);
	$total = total_entries($admins);
	echo "Indexing $total admins\n";

	// TODO - don't drop the entire index, just drop the type
	if ($options['purge']) {
		echo 'Purging type "admins" from index "woeplanet"' . "\n";
		$purge = array(
			'body' => array(
				'query' => array(
					'match_all' => array()
				)
			),
			'index' => 'woeplanet',
			'type' => 'admins'
		);
		$resp = $es->deleteByQuery($purge);
		$refresh = array(
			'index' => 'woeplanet'
		);
		$resp = $es->indices()->refresh($refresh);
	}

	$tsv = new TSVReader($admins);
	$row = 0;

	while(($data = $tsv->get()) !== false) {
		$row++;
		$params = array(
			'body' => array(
				'woeid' => $data['WOE_ID'],
				'iso' => $data['ISO'],
				'woeid_state' => $data['State'],
				'woeid_county' => $data['County'],
				'woeid_localadmin' => $data['Local_Admin'],
				'woeid_country' => $data['Country'],
				'woeid_continent' => $data['Continent']
			),
			'index' => 'woeplanet',
			'type' => 'admins',
			'id' => $data['WOE_ID']
		);
		$es->index($params);
		show_status($row, $total);
	}

	echo "\nIndexed $row of $total admins\n";
}

function index_placetypes($options, $es) {
	if ($options['purge']) {
		echo 'Purging type "placetypes" from index "woeplanet"' . "\n";
		$purge = array(
			'body' => array(
				'query' => array(
					'match_all' => array()
				)
			),
			'index' => 'woeplanet',
			'type' => 'placetypes'
		);
		$resp = $es->deleteByQuery($purge);
		$refresh = array(
			'index' => 'woeplanet'
		);
		$resp = $es->indices()->refresh($refresh);
	}

	$placetypes = array(
		array(
			'id' => 6,
			'name' => 'Street',
			'description' => 'A street',
			'shortname' => 'Street'
		),
		array (
			'id' => 7,
			'name' => 'Town',
			'description' => 'A populated settlement such as a city, town, village',
			'shortname' => 'Town'
		),
		array (
			'id' => 8,
			'name' => 'State',
			'description' => 'One of the primary administrative areas within a country',
			'shortname' => 'State'
		),
		array (
			'id' => 9,
			'name' => 'County',
			'description' => 'One of the secondary administrative areas within a country',
			'shortname' => 'County'
		),
		array (
			'id' => 10,
			'name' => 'Local Administrative Area',
			'description' => 'One of the tertiary administrative areas within a country',
			'shortname' => 'LocalAdmin'
		),
		array (
			'id' => 11,
			'name' => 'Postal Code',
			'description' => 'A partial or full postal code',
			'shortname' => 'Zip'
		),
		array (
			'id' => 12,
			'name' => 'Country',
			'description' => 'One of the countries or dependent territories defined by the ISO 3166-1 standard',
			'shortname' => 'Country'
		),
		array (
			'id' => 13,
			'name' => 'Island',
			'description' => 'An island',
			'shortname' => 'Island'
		),
		array(
			'id' => 14,
			'name' => 'Airport',
			'description' => 'An airport',
			'shortname' => 'Airport'
		),
		array (
			'id' => 15,
			'name' => 'Drainage',
			'description' => 'A water feature such as a river, canal, lake, bay, ocean',
			'shortname' => 'Drainage'
		),
		array (
			'id' => 16,
			'name' => 'Land Feature',
			'description' => 'A land feature such as a park, mountain, beach',
			'shortname' => 'LandFeature'
		),
		array (
			'id' => 17,
			'name' => 'Miscellaneous',
			'description' => 'A uncategorized place',
			'shortname' => 'Miscellaneous'
		),
		array (
			'id' => 18,
			'name' => 'Nationality',
			'description' => 'An area affiliated with a nationality',
			'shortname' => 'Nationality'
		),
		array (
			'id' => 19,
			'name' => 'Supername',
			'description' => 'An area covering multiple countries',
			'shortname' => 'Supername'
		),
		array (
			'id' => 20,
			'name' => 'Point of Interest',
			'description' => 'A point of interest such as a school, hospital, tourist attraction',
			'shortname' => 'POI'
		),
		array (
			'id' => 21,
			'name' => 'Region',
			'description' => 'An area covering portions of several countries',
			'shortname' => 'Region'
		),
		array (
			'id' => 22,
			'name' => 'Suburb',
			'description' => 'A subdivision of a town such as a suburb or neighborhood',
			'shortname' => 'Suburb'
		),
		array (
			'id' => 23,
			'name' => 'Sports Team',
			'description' => 'A sports team',
			'shortname' => 'Sports Team'
		),
		array (
			'id' => 24,
			'name' => 'Colloquial',
			'description' => 'A place known by a colloquial name',
			'shortname' => 'Colloquial'
		),
		array (
			'id' => 25,
			'name' => 'Zone',
			'description' => 'An area known within a specific context such as MSA or area code',
			'shortname' => 'Zone'
		),
		array (
			'id' => 26,
			'name' => 'Historical State',
			'description' => 'A historical primary administrative area within a country',
			'shortname' => 'HistoricalState'
		),
		array (
			'id' => 27,
			'name' => 'Historical County',
			'description' => 'A historical secondary administrative area within a country',
			'shortname' => 'HistoricalCounty'
		),
		array (
			'id' => 29,
			'name' => 'Continent',
			'description' => 'One of the major land masses on the Earth',
			'shortname' => 'Continent'
		),
		array (
			'id' => 31,
			'name' => 'Time Zone',
			'description' => 'An area defined by the Olson standard (tz database)',
			'shortname' => 'Timezone'
		),
		array (
			'id' => 32,
			'name' => 'Nearby Intersection',
			'description' => 'An intersection of streets that is nearby to the streets in a query string',
			'shortname' => 'Nearby Intersection'
		),
		array (
			'id' => 33,
			'name' => 'Estate',
			'description' => 'A housing development or subdivision known by name',
			'shortname' => 'Estate'
		),
		array (
			'id' => 35,
			'name' => 'Historical Town',
			'description' => 'A historical populated settlement that is no longer known by its original name',
			'shortname' => 'HistoricalTown'
		),
		array (
			'id' => 36,
			'name' => 'Aggregate',
			'description' => 'An aggregate place',
			'shortname' => 'Aggregate'
		),
		array (
			'id' => 37,
			'name' => 'Ocean',
			'description' => 'One of the five major bodies of water on the Earth',
			'shortname' => 'Ocean'
		),
		array (
			'id' => 38,
			'name' => 'Sea',
			'description' => 'An area of open water smaller than an ocean',
			'shortname' => 'Sea'
		)
	);

	$total = count($placetypes);
	$row = 0;
	echo "Indexing $total placetypes\n";

	$params = array(
		'body' => '',
		'index' => 'woeplanet',
		'type' => 'placetypes',
		'id' => ''
	);

	foreach ($placetypes as $placetype) {
		$row++;
		$params['body'] = $placetype;
		$params['id'] = $placetype['id'];

		$es->index($params);
		show_status($row, $total);
	}

	echo "\nIndexed $row of $total placetypes\n";
	exit;
}

function get_by_woeid($woeid, $es) {
	$query = array(
		'index' => 'woeplanet',
		'type' => 'places',
		'id' => $woeid,
		'ignore' => 404
	);

	return $es->get($query);
}

function total_entries($file) {
	$handle = fopen($file, "r");
	$count = 0;
	while (fgets($handle)) {
		$count++;
	}
	fclose($handle);
	return --$count;
}

// Thanks to Brian Moon for this - http://brian.moonspot.net/php-progress-bar
function show_status($done, $total, $size=30) {
	if ($done === 0) {
		$done = 1;
	}
	static $start_time;
	if ($done > $total)
		return; // if we go over our bound, just ignore it
	if (empty ($start_time))
		$start_time = time();
	$now = time();
	$perc = (double) ($done / $total);
	$bar = floor($perc * $size);
	$status_bar = "\r[";
	$status_bar .= str_repeat("=", $bar);
	if ($bar < $size) {
		$status_bar .= ">";
		$status_bar .= str_repeat(" ", $size - $bar);
	} else {
		$status_bar .= "=";
	}
	$disp = number_format($perc * 100, 0);
	$status_bar .= "] $disp%  $done/$total";
	if ($done === 0){$done = 1;}//avoid div zero warning
	$rate = ($now - $start_time) / $done;
	$left = $total - $done;
	$eta = round($rate * $left, 2);
	$elapsed = $now - $start_time;
	//$status_bar .= " remaining";
	//$status_bar .= " remaining: " . number_format($eta) . " sec. elapsed: " . number_format($elapsed) . " sec.";

	echo "$status_bar  ";
	flush();
	// when done, send a newline
	if($done == $total) {
	    echo "\n";
	}
}

class TSVReader {
	private $handle = NULL;
	private $line_length = 1000;
	private $separator = "\t";
	private $header = NULL;

	public function __construct($path) {
		if (($this->handle = fopen($path, "r")) === false) {
			throw new Exception('Failed to open ' . $path);
		}

		$this->header = fgetcsv($this->handle, $this->line_length, $this->separator);
	}

	public function get() {
		if (($data = fgetcsv($this->handle, $this->line_length, $this->separator)) !== false) {
			$row = array();
			foreach ($this->header as $i => $column) {
				$row[$column] = $data[$i];
			}

			return $row;
		}

		return false;
	}

	public function close() {
		fclose($this->handle);
		$this->handle = NULL;
	}
}

function get_options() {
	$shortopts = "vpe:g:s:";
	$longopts = array(
		"verbose",
		"purge",
		"elasticsearch:",
		"geoplanet:",
		"stage:"
	);
	$opts = array(
		'elasticsearch' => 'http://localhost:9200',
		'geoplanet' => '',
		'stage' => 'all',
		'verbose' => false,
		'purge' => false
	);
	$options = getopt($shortopts, $longopts);

	if (isset($options['v']) || isset($options['verbose'])) {
		$opts['verbose'] = true;
	}
	if (isset($options['p']) || isset($options['purge'])) {
		$opts['purge'] = true;
	}
	if (isset($options['e'])) {
		$opts['elasticsearch'] = $options['e'];
	}
	else if (isset($options['elasticsearch'])) {
		$opts['elasticsearch'] = $options['elasticsearch'];
	}
	if (isset($options['s'])) {
		$opts['stage'] = $options['s'];
	}
	elseif (isset($options['stage'])) {
		$opts['stage'] = $options['stage'];
	}
	if (isset($options['g'])) {
		$opts['geoplanet'] = $options['g'];
	}
	else if (isset($options['geoplanet'])) {
		$opts['geoplanet'] = $options['geoplanet'];
	}
	else {
		echo "Missing path to GeoPlanet Data\n";
		exit;
	}

	return $opts;
}

?>
