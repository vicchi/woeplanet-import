#!/usr/bin/env php
<?php

require 'vendor/autoload.php';
require 'geoplanet-data-reader.php';
require 'woeplanet-placetypes.php';

$shortopts = "vpe:g:s:";
$longopts = array(
	"verbose",
	"purge",
	"elasticsearch:",
	"geoplanet:",
	"stage:"
);

$verbose = false;
$purge = false;
$path = NULL;
$instance = 'http://localhost:9200';
$stage = NULL;

$options = getopt($shortopts, $longopts);

if (isset($options['v']) || isset($options['verbose'])) {
	$verbose = true;
}
if (isset($options['p']) || isset($options['purge'])) {
	$purge = true;
}
if (isset($options['e'])) {
	$instance = $options['e'];
}
else if (isset($options['elasticsearch'])) {
	$instance = $options['elasticsearch'];
}

if (isset($options['g'])) {
	$path = $options['g'];
}
else if (isset($options['geoplanet'])) {
	$path = $options['geoplanet'];
}
else {
	echo "Missing path to GeoPlanet Data\n";
	exit;
}

if (isset($options['s'])) {
	$stage = $options['s'];
}
else if (isset($options['stage'])) {
	$stage = $options['stage'];
}

$importer = new GeoPlanetImporter($instance, $path, $verbose, $purge, $stage);
$importer->run();

class GeoPlanetImporter {
	const INDEX = "woeplanet";

	const PLACES_TYPE = "places";
	const ADMINS_TYPE = "admins";
	const PLACETYPES_TYPE = "placetype";
	const META_TYPE = 'meta';

	const SETUP_STAGE = 'setup';
	const PLACES_STAGE = 'places';
	const COORDS_STAGE = 'coords';
	const CHANGES_STAGE = 'changes';
	const ADJACENCIES_STAGE = 'adjacencies';
	const ALIASES_STAGE = 'aliases';
	const ADMINS_STAGE = 'admins';
	const PLACETYPES_STAGE = 'placetypes';
	const TEST_STAGE = 'test';

	const PLACES_WOEID = 'WOE_ID';
	const PLACES_ISO = 'ISO';
	const PLACES_NAME = 'Name';
	const PLACES_LANGUAGE = 'Language';
	const PLACES_PLACETYPE = 'PlaceType';
	const PLACES_PARENTID = 'Parent_ID';

	const COORDS_WOEID = 'WOE_ID';
	const COORDS_LAT = 'Lat';
	const COORDS_LON = 'Lon';
	const COORDS_NELAT = 'NE_Lat';
	const COORDS_NELON = 'NE_Lon';
	const COORDS_SWLAT = 'SW_Lat';
	const COORDS_SWLON = 'SW_Lon';

	const CHANGES_WOEID = 'Woe_id';
	const CHANGES_REPID = 'Rep_id';

	const ADJACENCIES_WOEID = 'Place_WOE_ID';
	const ADJACENCIES_NEIGHBOUR = 'Neighbour_WOE_ID';

	const ALIAS_WOEID = 'WOE_ID';
	const ALIAS_NAME = 'Name';
	const ALIAS_TYPE = 'Name_Type';
	const ALIAS_LANG = 'Language';

	const ADMINS_WOEID = 'WOE_ID';
	const ADMINS_ISO = 'ISO';
	const ADMINS_STATE = 'State';
	const ADMINS_COUNTY = 'County';
	const ADMINS_LOCALADMIN = 'Local_Admin';
	const ADMINS_COUNTRY = 'County';
	const ADMINS_CONTINENT = 'Continent';

	private $verbose = false;
	private $purge = false;
	private $path = NULL;
	private $instance = 'http://localhost:9200';
	private $stage = NULL;
	private $source;
	private $version;
	private $timestamp;
	private $history;

	private $es;
	private $sqlite;
	private $stages;

	private $places;
	private $aliases;
	private $adjacencies;
	private $changes;
	private $coords;
	private $admins;

	private $files;

	private $placetypes;

	public function __construct($instance, $path, $verbose, $purge, $stage=NULL) {
		$this->placetypes = new WoePlanetPlaceTypes();

		$this->instance = $instance;
		$this->path = $path;
		$this->verbose = $verbose;
		$this->purge = $purge;

		$stage = strtolower($stage);
		if ($stage == NULL) {
			$stage = 'run';
		}
		$this->stage = $stage;
		$this->es = new Elasticsearch\Client();
		$this->sqlite = array();

		$this->stages = array(
			self::SETUP_STAGE,
			self::PLACES_STAGE,
			self::COORDS_STAGE,
			self::CHANGES_STAGE,
			self::ADJACENCIES_STAGE,
			self::ALIASES_STAGE,
			self::ADMINS_STAGE,
			self::PLACETYPES_STAGE,
			self::TEST_STAGE
		);

		foreach ($this->stages as $stage) {
			$path= 'geoplanet_' . $stage . '.sqlite3';
			$this->sqlite[$stage] = array(
				'path' => $path,
				'handle' => NULL
			);
		}

		$match = array();
		$pattern = '/(.+)_([\d\.]+)$/';
		$ret = preg_match($pattern, basename($this->path), $match);
		if ($ret === 1) {
			$this->source = $match[1];
			$this->version = $match[2];
			$this->timestamp = time();
			$this->history = array(
				array(
					'source' => sprintf('%s %s', $this->source, $this->version),
					'timestamp' => (int) $this->timestamp
				)
			);
		}

		else {
			$this->log('Can\t get source and version from path ' . $this->path);
			exit;
		}

		$this->places = sprintf('geoplanet_places_%s.tsv', $this->version);
		$this->aliases = sprintf('geoplanet_aliases_%s.tsv', $this->version);
		$this->adjacencies = sprintf('geoplanet_adjacencies_%s.tsv', $this->version);
		$this->changes = sprintf('geoplanet_changes_%s.tsv', $this->version);
		$this->coords = sprintf('geoplanet_coords_%s.tsv', $this->version);
		$this->admins = sprintf('geoplanet_admins_%s.tsv', $this->version);

		$this->files = array();

		$dir = opendir($this->path);
		while (false !== ($entry = readdir($dir))) {
			if ($entry === '.' || $entry === '..')
				continue;

			$this->files[$entry] = $this->path . DIRECTORY_SEPARATOR . $entry;
		}

		if (false === array_key_exists($this->places, $this->files)) {
			$this->log("Missing $this->places");
			exit;
		}
	}

	/***************************************************************************
	 * run
	 */

	public function run() {
		$this->elapsed('run');

		if ($this->stage == 'run' || $this->stage == 'setup') {
			$this->setup();
			if ($this->stage == 'setup')
				exit;
		}

		if ($this->stage == 'run' || $this->stage == 'coords') {
			$this->index_coords();
			if ($this->stage != 'run')
				exit;
		}

		if ($this->stage == 'run' || $this->stage == 'places') {
			$this->index_places();
			if ($this->stage != 'run')
				exit;
		}

		if ($this->stage =='run' || $this->stage == 'changes') {
			$this->index_changes();
			if ($this->stage != 'run')
				exit;
		}

		if ($this->stage == 'run' || $this->stage == 'adjacencies') {
			$this->index_adjacencies();
			if ($this->stage != 'run')
				exit;
		}

		if ($this->stage == 'run' || $this->stage == 'aliases') {
			$this->index_aliases();
			if ($this->stage != 'run')
				exit;
		}

		if ($this->stage == 'run' || $this->stage == 'admins') {
			$this->index_admins();
			if ($this->stage != 'run')
				exit;
		}

		if ($this->stage == 'run' || $this->stage == 'placetypes') {
			$this->index_placetypes();
			if ($this->stage != 'run')
				exit;
		}

		if ($this->stage != 'run' && $this->stage == 'test') {
			$this->run_test();
		}

		$elapsed = $this->seconds_to_time($this->elapsed('run'));
		$this->log("Completed in $elapsed");
	}

	/***************************************************************************
	 * setup
	 */

	private function setup() {
		$this->log("Creating Elasticsearch index and type mappings");
		$this->elapsed('setup');

		if ($this->purge) {
			$this->logVerbose("Dropping index " . self::INDEX);

			$params = array(
				'index' => self::INDEX,
				'ignore' => 404
			);
			$this->es->indices()->delete($params);
			$this->logVerbose("Index " . self::INDEX . " dropped");
		}

		$params = array(
			'index' => self::INDEX
		);

		$this->logVerbose("Checking index " . self::INDEX);
		if ($this->es->indices()->exists($params)) {
			$this->logVerbose("Index ". self::INDEX . " exists, skipping index and mapping creation");
		}

		else {
			$body = array(
				'mappings' => array(
					self::PLACES_TYPE => array(
						'_timestamp' => array(
							'enabled' => true
						),
						'properties' => array(
							'type' => array(
								'type' => 'string'
							),
							'features' => array(
								'properties' => array(
									'type' => array(
										'type' => 'string'
									),
									'id' => array(
										'type' => 'long'
									),
									'bbox' => array(
										'type' => 'double'
									),
									'geometry' => array(
										'properties' => array(
											'coordinates' => array(
												'type' => 'geo_point'
											),
											'type' => array(
												'type' => 'string'
											)
										)
									),
									'properties' => array(
										'properties' => array(
											'woe:id' => array(
												'type' => 'long'
											),
											// was [woe]id_supersedes
											'woe:supersedes' => array(
												'type' => 'long'
											),
											// was [woe]id_superseded_by
											'woe:superseded' => array(
												'type' => 'long'
											),
											'bounds' => array(
												'type' => 'geo_shape'
											),
											'history' => array(
												'properties' => array(
													'source' => array(
														'type' => 'string'
													),
													'timestamp' => array(
														'type' => 'long'
													)
												)
											),
											'iso' => array(
												'type' => 'string'
											),
											'lang' => array(
												'type' => 'string'
											),
											'name' => array(
												'type' => 'string'
											),
											'woe:type' => array(
												'type' => 'integer'
											),
											'woe:placetype' => array(
												'type' => 'string'
											),
											'woe:parent' => array(
												'type' => 'long'
											),
											'woe:adjacent' => array(
												'type' => 'long'
											),
											'alias_Q' => array(
												'properties' => array(
													'name' => array(
														'type' => 'string'
													),
													'lang' => array(
														'type' => 'string'
													)
												)
											),
											'alias_V' => array(
												'properties' => array(
													'name' => array(
														'type' => 'string'
													),
													'lang' => array(
														'type' => 'string'
													)
												)
											),
											'alias_A' => array(
												'properties' => array(
													'name' => array(
														'type' => 'string'
													),
													'lang' => array(
														'type' => 'string'
													)
												)
											),
											'alias_S' => array(
												'properties' => array(
													'name' => array(
														'type' => 'string'
													),
													'lang' => array(
														'type' => 'string'
													)
												)
											)
										)	// end-properties
									)	// end-properties
								) // end-properties
							)	// end-features
						)	// end-properties
					),
					self::ADMINS_TYPE => array(
						'_timestamp' => array(
							'enabled' => true
						),
						'properties' => array(
							'woe:id' => array(
								'type' => 'long'
							),
							'woe:continent' => array(
								'type' => 'long'
							),
							'woe:country' => array(
								'type' => 'long'
							),
							'woe:county' => array(
								'type' => 'long'
							),
							'woe:localadmin' => array(
								'type' => 'long'
							),
							'woe:state' => array(
								'type' => 'long'
							)
						)
					),
					self::PLACETYPES_TYPE => array(
						'_timestamp' => array(
							'enabled' => true
						)
					)
				)
			);

			$params['body'] = $body;
			$this->logVerbose("Creating index " . self::INDEX);
			$this->es->indices()->create($params);
		}

		$elapsed = $this->seconds_to_time($this->elapsed('setup'));
		$this->log("Completed Elasticsearch index and type mappings in $elapsed");
	}

	/***************************************************************************
	 * index_places
	 */

	private function index_places() {
		$this->elapsed('places');

		$has_coords = true;
		if (false === array_key_exists($this->coords, $this->files)) {
			$this->log("Missing $this->coords: skipping coordinates");
			$has_coords = false;
		}

		if ($has_coords) {
			if ($this->sqlite[self::COORDS_STAGE]['handle'] === NULL) {
				$name = 'sqlite:' . $this->sqlite[self::COORDS_STAGE]['path'];
				$this->sqlite[self::COORDS_STAGE]['handle'] = new PDO($name);
			}
			$db = $this->sqlite[self::COORDS_STAGE]['handle'];
		}

		$tsv = new GeoPlanetDataReader();
		$tsv->open($this->files[$this->places]);
		$total = $tsv->size();

		if ($this->purge) {
			$params = array(
				'body' => array(
					'query' => array(
						'bool' => array(
							'must' => array(
								'match_all' => array()
							)
						)
					)
				),
				'index' => self::INDEX,
				'type' => self::PLACES_TYPE,
			);
			$this->logVerbose('Purging type "places" from index "woeplanet"');
			$this->es->deleteByQuery($params);
			$params = array(
				'index' => self::INDEX
			);
			$this->logVerbose('Refreshing index "woeplanet"');
			$this->es->indices()->refresh($params);
		}

		$this->log("Indexing places");

		$max_woeid = 0;

		$row = 0;
		$select = "SELECT * FROM geoplanet_coords WHERE woeid = :woeid";

		while (($data = $tsv->get()) !== false) {
			$row++;
			$this->check_raw_places($row, $data);
			$old =$this->get_by_woeid($data['WOE_ID']);

			$placetype = $this->placetypes->get_by_shortname($data['PlaceType']);
			if (!$placetype['found']) {
				throw new Exception('Cannot find match for placetype ' . $data['PlaceType'] . ' for WOEID ' . $data['WOE_ID']);
			}


			$params = array(
				'body' => array(
					'type' => 'FeatureCollection',
					'features' => array(
						array(
							'type' => 'Feature',
							'id' => (int) $data['WOE_ID'],
							/*'bbox' => array(0, 0, 0, 0),*/
							'geometry' => array(
								'type' => 'Point',
								'coordinates' => array(0, 0)
							),
							'properties' => array(
								'woe:id' => (int) $data['WOE_ID'],
								'iso' => $data['ISO'],
								'name' => $data['Name'],
								'lang' => $data['Language'],
								'woe:type' => (int) $placetype['placetype']['id'],
								'woe:placetype' => $placetype['placetype']['name'],
								'woe:parent' => (int) $data['Parent_ID'],
								/*'bounds' => array(
									'type' => 'Polygon',
									'coordinates' => array(
										array(
											array(0, 0),
											array(0, 0)
										)
									)
								),*/
								'history' => $this->history
							)
						)
					)
				),
				'index' => self::INDEX,
				'type' => self::PLACES_TYPE,
				'id' => (int) $data['WOE_ID']
			);

			if ($has_coords) {
				$statement = $db->prepare($select);
				$statement->bindParam(':woeid', $data['WOE_ID']);
				$statement->execute();
				$coords = $statement->fetch(PDO::FETCH_ASSOC);
				if ($coords) {
					$params['body']['features'][0]['geometry']['coordinates'] = array((double) $coords['lon'], (double) $coords['lat']);

					$bbox = new BoundingBox((double) $coords['swlon'], (double) $coords['swlat'], (double) $coords['nelon'], (double) $coords['nelat']);
					if (!$bbox->is_empty()) {
						$params['body']['features'][0]['bbox'] = $bbox->to_geojson();
						$params['body']['features'][0]['properties']['bounds'] = $bbox->to_polygon();
					}
				}
			}

			if ($old['found']) {
				$history = array();
				if (isset($old['_source']['features'][0]['properties']['history'])) {
					$history = $old['_source']['features'][0]['properties']['history'];
				}
				$history[] = $this->history;
				$params['body']['features'][0]['properties']['history'] = $history;

				if (isset($old['_source']['features'][0]['properties']['woe:supersedes'])) {
					$params['body']['features'][0]['properties']['woe:supersedes'] = $old['_source']['features'][0]['properties']['woe:supersedes'];
				}

				if (isset($old['_source']['features'][0]['properties']['woe:superseded'])) {
					$params['body']['features'][0]['properties']['woe:superseded'] = $old['_source']['features'][0]['properties']['woe:superseded'];
				}
			}

			$this->es->index($params);
			$this->show_status($row, $total);

			if ($max_woeid <= $data['WOE_ID']) {
				$max_woeid = $data['WOE_ID'];
			}
		}

		$params = array(
			'body' => array(
				'max_woeid' => $max_woeid
			),
			'index' => self::INDEX,
			'type' => self::META_TYPE,
			'id' => 1
		);
		$this->es->index($params);

		$elapsed = $this->seconds_to_time($this->elapsed('places'));
		$this->log("Completed indexing places in $elapsed");
	}

	/***************************************************************************
 	* index_coords
 	*/

	private function index_coords() {
		$this->elapsed('coords');

		$has_coords = true;
		if (false === array_key_exists($this->coords, $this->files)) {
			$this->log("Missing $this->coords: skipping coordinates");
			$has_coords = false;
		}

		if ($has_coords) {
			if ($this->sqlite[self::COORDS_STAGE]['handle'] === NULL) {
				$name = 'sqlite:' . $this->sqlite[self::COORDS_STAGE]['path'];
				$this->sqlite[self::COORDS_STAGE]['handle'] = new PDO($name);
			}
			$db = $this->sqlite[self::COORDS_STAGE]['handle'];
			$tsv = new GeoPlanetDataReader();

			$tsv->open($this->files[$this->coords]);
			$total = $tsv->size();

			if ($this->purge) {
				$this->logVerbose('Purging table "geoplanets_coords" from cache');
				$drop = "DROP TABLE geoplanet_coords";
				$db->exec($drop);
			}

			$this->log("Caching $total coordinates");

			$setup = "CREATE TABLE geoplanet_coords(
				woeid INTEGER PRIMARY KEY,
				lat REAL,
				lon REAL,
				swlat REAL,
				swlon REAL,
				nelat REAL,
				nelon REAL);";
			$db->exec($setup);

			$row = 0;

			while (($data = $tsv->get()) !== false) {
				$row++;
				$this->check_raw_coords($row, $data);

				$insert = "INSERT INTO geoplanet_coords(woeid,lat,lon,swlat,swlon,nelat,nelon)
					VALUES(:woeid,:lat,:lon,:swlat,:swlon,:nelat,:nelon)";

				$statement = $db->prepare($insert);
				$statement->bindParam(':woeid', $data['WOE_ID']);

				$lat = $this->sanitize_coord($data['Lat']);
				$lon = $this->sanitize_coord($data['Lon']);
				$statement->bindParam(':lat', $lat);
				$statement->bindParam(':lon', $lon);

				$nelat = $this->sanitize_coord($data['NE_Lat']);
				$nelon = $this->sanitize_coord($data['NE_Lon']);
				$statement->bindParam(':nelat', $nelat);
				$statement->bindParam(':nelon', $nelon);

				$swlat = $this->sanitize_coord($data['SW_Lat']);
				$swlon = $this->sanitize_coord($data['SW_Lon']);
				$statement->bindParam(':swlat', $swlat);
				$statement->bindParam(':swlon', $swlon);

				$statement->execute();

				$this->show_status($row, $total);
			}

			$tsv->close();
		}

		$elapsed = $this->seconds_to_time($this->elapsed('coords'));
		$this->log("Completed indexing coordinates in $elapsed");
	}

	/***************************************************************************
 	* index_changes
	 */

	private function index_changes() {
		$this->elapsed('changes');
		$row = 0;
		$total = 0;

		$has_changes = true;
		if (false === array_key_exists($this->changes, $this->files)) {
			$this->log("Missing $this->changes: skipping changes");
			$has_changes = false;
		}

		if ($has_changes) {
			$tsv = new GeoPlanetDataReader();
			$tsv->open($this->files[$this->changes]);
			$total = $tsv->size();

			$this->log("Indexing $total changes");
			$row = 0;

			$logs = array();
			while (($data = $tsv->get()) !== false) {
				$row++;
				$this->check_raw_changes($row, $data);

				// Hey, let's be consistent in our column naming convention ... WTF
				$old_woeid = $data['Woe_id'];
				$new_woeid = $data['Rep_id'];

				$old = $this->get_by_woeid($old_woeid);
				$new = $this->get_by_woeid($new_woeid);

				if ($old['found']) {
					$params = array(
						'body' => $old['_source'],
						'index' => self::INDEX,
						'type' => self::PLACES_TYPE,
						'id' => $old_woeid
					);
					$params['body']['features'][0]['properties']['woe:superseded'] = $new_woeid;

					$this->es->index($params);
				}
				else {
					$logs[] = "WTF ... no document found for old WOEID $new_woeid";
				}

				if ($new['found']) {
					$supersedes = array();
					if (isset($new['body']['features'][0]['properties']['woe:supersedes'])) {
						$supersedes = $new['body']['features'][0]['properties']['woe:superseded'];
					}
					if (array_search($old_woeid, $supersedes) == false) {
						$supersedes[] = $old_woeid;

						$params = array(
							'body' => $new['_source'],
							'index' => self::INDEX,
							'type' => self::PLACES_TYPE,
							'id' => $new_woeid
						);
						$params['body']['features'][0]['properties']['woe:supersedes'] = $superseded;

						$this->es->index($params);
					}
				}

				else {
					$logs[] = "WTF ... no record found for new WOEID $new_woeid";
				}

				$this->show_status($row, $total);
			}

			if (count($logs) > 0) {
				foreach ($logs as $log) {
					$this->log($log);
				}
			}
		}

		$this->log("\nIndexed $row of $total changes");
		$elapsed = $this->seconds_to_time($this->elapsed('changes'));
		$this->log("Completed indexing changes in $elapsed");
	}

	/***************************************************************************
	 * index_adjacencies
	 */

	private function index_adjacencies() {
		$this->elapsed('adjacencies');

		if ($this->sqlite[self::ADJACENCIES_STAGE]['handle'] === NULL) {
			$name = 'sqlite:' . $this->sqlite[self::ADJACENCIES_STAGE]['path'];
			$this->sqlite[self::ADJACENCIES_STAGE]['handle'] = new PDO($name);
		}
		$db = $this->sqlite[self::ADJACENCIES_STAGE]['handle'];
		$tsv = new GeoPlanetDataReader();

		$tsv->open($this->files[$this->adjacencies]);
		$total = $tsv->size();

		if ($this->purge) {
			$this->logVerbose('Purging table "geoplanet_adjacencies" from cache');
			$drop = "DROP TABLE geoplanet_adjacencies";
			$db->exec($drop);
		}

		$this->log("Caching $total adjacencies");

		$setup = "CREATE TABLE geoplanet_adjacencies (
			woeid INTEGER,
			neighbour INTEGER
		)";
		$db->exec($setup);

		$setup = "CREATE INDEX adjacencies_by_woeid ON geoplanet_adjacencies (woeid)";
		$db->exec($setup);

		$row = 0;
		$insert = "INSERT INTO geoplanet_adjacencies(woeid, neighbour) VALUES(:woeid,:neighbour)";
		$statement = $db->prepare($insert);

		while (($data = $tsv->get()) !== false) {
			$row++;
			$this->check_raw_adjacencies($row, $data);

			$statement->bindParam(':woeid', $data['Place_WOE_ID']);
			$statement->bindParam(':neighbour', $data['Neighbour_WOE_ID']);

			$statement->execute();

			$this->show_status($row, $total);
		}

		$this->logVerbose("\nCached $row of $total adjacencies");

		$ids = array();

		$select = "SELECT COUNT(DISTINCT woeid) FROM geoplanet_adjacencies";
		$statement = $db->prepare($select);
		$statement->execute();
		$result = $statement->fetch(PDO::FETCH_ASSOC);
		$count = $result['COUNT(DISTINCT woeid)'];

		$this->log("Indexing $count adjacencies");

		$select = "SELECT DISTINCT(woeid) FROM geoplanet_adjacencies";
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

			$doc = $this->get_by_woeid($adj['woeid']);
			$params = array(
				'body' => $doc['_source'],
				'index' => self::INDEX,
				'type' => self::PLACES_TYPE,
				'id' => $adj['woeid']
			);
			$params['body']['features'][0]['properties']['woe:adjacent'] = $adjacent;

			$this->es->index($params);

			$this->show_status($row, $count);
		}

		$this->log("\nIndexed $row of $count adjacencies");

		$elapsed = $this->seconds_to_time($this->elapsed('adjacencies'));
		$this->log("Completed indexing adjacencies in $elapsed");
	}

	/***************************************************************************
 	* index_aliases
	 */

	private function index_aliases() {
		$this->elapsed('aliases');

		if ($this->sqlite[self::ALIASES_STAGE]['handle'] === NULL) {
			$name = 'sqlite:' . $this->sqlite[self::ALIASES_STAGE]['path'];
			$this->sqlite[self::ALIASES_STAGE]['handle'] = new PDO($name);
		}
		$db = $this->sqlite[self::ALIASES_STAGE]['handle'];
		$tsv = new GeoPlanetDataReader();

		$tsv->open($this->files[$this->aliases]);
		$total = $tsv->size();

		if ($this->purge) {
			$this->logVerbose('Purging table "geoplanet_aliases" from cache');
			$drop = "DROP TABLE geoplanet_aliases";
			$db->exec($drop);
		}

		$this->log("Caching and aggregating $total aliases");

		$setup = "CREATE TABLE IF NOT EXISTS geoplanet_aliases (
			woeid INTEGER,
			name TEXT,
			type TEXT,
			lang TEXT
		);";
		$db->exec($setup);

		$setup = "CREATE INDEX aliases_by_woeid ON geoplanet_aliases(woeid);";
		$db->exec($setup);

		$row = 0;
		$insert = "INSERT INTO geoplanet_aliases(woeid, name, type, lang) VALUES(:woeid,:name,:type,:lang)";
		$statement = $db->prepare($insert);

		while (($data = $tsv->get()) !== false) {
			$row++;
			$this->check_raw_aliases($row, $data);

			$statement->bindParam(':woeid', $data['WOE_ID']);
			$statement->bindParam(':name', $data['Name']);
			$statement->bindParam(':type', $data['Name_Type']);
			$statement->bindParam(':lang', $data['Language']);

			$statement->execute();
			$this->show_status($row, $total);
		}

		$this->logVerbose("\nCached $row of $total aliases");

		$select = "SELECT COUNT(DISTINCT woeid) FROM geoplanet_aliases";
		$statement = $db->prepare($select);
		$statement->execute();
		$result = $statement->fetch(PDO::FETCH_ASSOC);
		$count = $result['COUNT(DISTINCT woeid)'];

		$this->log("Indexing $count aliases");

		$select = "SELECT DISTINCT(woeid) FROM geoplanet_aliases";
		$distinct = $db->prepare($select);
		$distinct->execute();

		$row = 0;
		$ids =array();
		$this->log("Aggregating $count candidate WOEIDs");
		while (($woeid = $distinct->fetch(PDO::FETCH_ASSOC)) !== false) {
			$row++;
			$ids[] = $woeid['woeid'];
			$this->show_status($row, $count);
		}
		$total = count($ids);
		$this->log("Found $total candidate WOEIDs; indexing ...");

		$select = "SELECT * FROM geoplanet_aliases WHERE woeid = :woeid";
		$statement = $db->prepare($select);
		$row = 0;

		foreach ($ids as $id) {
			$row++;
			$statement->bindParam(':woeid', $id);
			$statement->execute();

			$aliases = array();
			while (($alias = $statement->fetch(PDO::FETCH_ASSOC)) !== false) {
				$woeid = $alias['woeid'];
				$name = $alias['name'];
				$type = $alias['type'];
				$lang = $alias['lang'];
				$key = sprintf('woe:alias_%s', $type);

				$names = array();
				if (array_key_exists($key, $aliases)) {
					$names = $aliases[$key];
				}

				$element = array(
					'name' => $name,
					'lang' => $lang
				);
				if (!in_array($element, $names, true)) {
					$names[] = $element;
				}
				$aliases[$key] = $names;
			}

			$doc = $this->get_by_woeid($id);
			$params = array(
				'body' => $doc['_source'],
				'index' => self::INDEX,
				'type' => self::PLACES_TYPE,
				'id' => $id
			);


			foreach ($aliases as $key => $value) {
				$params['body']['features'][0]['properties'][$key] = $value;
			}

			$this->es->index($params);
			$this->show_status($row, $total);
		}

		$this->log("\nIndexed $row of $count aliases");

		$elapsed = $this->seconds_to_time($this->elapsed('aliases'));
		$this->log("Completed indexing aliases in $elapsed");
	}

	/***************************************************************************
 	* index_admins
 	*/

	private function index_admins() {
		$this->log("Indexing admins");
		$this->elapsed('admins');

		$row = 0;
		$total = 0;

		$has_admins = true;
		if (false === array_key_exists($this->admins, $this->files)) {
			$this->log("Missing $this->admins: skipping admins");
			$has_admins = false;
		}

		if ($has_admins) {
			$tsv = new GeoPlanetDataReader();

			$tsv->open($this->files[$this->admins]);
			$total = $tsv->size();

			if ($this->purge) {
				$this->logVerbose('Purging type "admins" from index "woeplanet"');
				$purge = array(
					'body' => array(
						'query' => array(
							'match_all' => array()
						)
					),
					'index' => self::INDEX,
					'type' => self::ADMINS_TYPE,
					'ignore' => 404
				);
				$resp = $this->es->deleteByQuery($purge);
				$refresh = array(
					'index' => self::INDEX
				);
				$resp = $this->es->indices()->refresh($refresh);
			}

			$row = 0;

			while(($data = $tsv->get()) !== false) {
				$row++;
				$this->check_raw_admins($row, $data);

				$params = array(
					'body' => array(
						'woe:id' => $data['WOE_ID'],
						'iso' => $data['ISO'],
						'woe:state' => $data['State'],
						'woe:county' => $data['County'],
						'woe:localadmin' => $data['Local_Admin'],
						'woe:country' => $data['Country'],
						'woe:continent' => $data['Continent']
					),
					'index' => self::INDEX,
					'type' => self::ADMINS_TYPE,
					'id' => $data['WOE_ID']
				);
				$this->es->index($params);
				$this->show_status($row, $total);
			}
		}

		$elapsed = $this->seconds_to_time($this->elapsed('admins'));
		$this->log("Completed indexing admins in $elapsed");
	}

	/***************************************************************************
	 * index_placetypes
	 */

	private function index_placetypes() {
		$this->log("Indexing placetypes");
		$this->elapsed('placetypes');

		if ($this->purge) {
			$this->logVerbose('Purging type "placetypes" from index "woeplanet"');
			$purge = array(
				'body' => array(
					'query' => array(
						'match_all' => array()
					)
				),
				'index' => 'woeplanet',
				'type' => 'placetypes',
				'ignore' => 404
			);
			$resp = $this->es->deleteByQuery($purge);
			$refresh = array(
				'index' => 'woeplanet'
			);
			$resp = $this->es->indices()->refresh($refresh);
		}

		$placetypes = $this->placetypes->get();

		$total = count($placetypes);
		$row = 0;
		$this->log("Indexing $total placetypes");

		$params = array(
			'body' => '',
			'index' => self::INDEX,
			'type' => self::PLACETYPES_TYPE,
			'id' => ''
		);

		foreach ($placetypes as $placetype) {
			$row++;
			$params['body'] = $placetype;
			$params['id'] = $placetype['id'];

			$this->es->index($params);
			$this->show_status($row, $total);
		}

		$this->log("\nIndexed $row of $total placetypes");
		$elapsed = $this->seconds_to_time($this->elapsed('placetypes'));
		$this->log("Completed indexing placetypes in $elapsed");
	}

	private function run_test() {
		$this->log("Running test code");
		$this->elapsed('test');

		$woeid = 44418;

		$entry = $this->get_by_woeid($woeid);
		$this->log('entry dump');
		$this->log(var_export($entry, true));

		$source = &$entry['_source'];
		$feature = &$source['features'][0];

		$this->log('feature dump');
		$this->log(var_export($feature, true));

		$feature['properties']['woe:adjacent'] = array(1, 2, 3, 4);

		$this->log('updated dump');
		$this->log(var_export($feature, true));

		$this->log('updated entry');
		$this->log(var_export($entry, true));

		$elapsed = $this->seconds_to_time($this->elapsed('test'));
		$this->log("Completed test run in $elapsed");
	}

	/***************************************************************************
	 * get_by_woeid
	 */

	private function get_by_woeid($woeid) {
		$query = array(
			'index' => self::INDEX,
			'type' => self::PLACES_TYPE,
			'id' => $woeid,
			'ignore' => 404
		);

		$ret = $this->es->get($query);
		if (is_string($ret)) {
			$ret = json_decode($ret, true);
		}
		return $ret;
	}

	/***************************************************************************
 	* elapsed
 	*/

	private function elapsed($stage) {
		static $last = array(
			'run' => null,
			'setup' => null,
			'places' => null,
			'coords' => null,
			'changes' => null,
			'adjacencies' => null,
			'aliases' => null,
			'admins' => null,
			'placetypes' => null,
			'test' => null
		);

		//$now = microtime(true);
		$now = time();
		$elapsed = $now;
		if ($last[$stage] != null) {
			$elapsed = ($now - $last[$stage]);
		}

		$last[$stage] = $now;
		return $elapsed;
	}

	/***************************************************************************
	 * seconds_to_time
	 */

	private function seconds_to_time($seconds) {
		$dtf = new DateTime("@0");
		$dtt = new DateTime("@$seconds");
		return $dtf->diff($dtt)->format('%h hours, %i mins, %s secs');
	}

	private function log($message) {
		echo "$message\n";
	}

	private function logVerbose($message) {
		if ($this->verbose) {
			$this->log($message);
		}
	}

	private function sanitize_coord($coord) {
		if ($coord == '\N' || $coord == '\n' || $coord == NULL) {
			$coord = 0;
		}

		return $coord;
	}

	// Thanks to Brian Moon for this - http://brian.moonspot.net/php-progress-bar
	private function show_status($done, $total, $size=30) {
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

		echo "$status_bar  ";
		flush();
		// when done, send a newline
		if($done == $total) {
			echo "\n";
		}
	}

	private function check_raw_places($row, $data) {
		$fields = array(self::PLACES_WOEID, self::PLACES_ISO,
			self::PLACES_NAME, self::PLACES_LANGUAGE,
			self::PLACES_PLACETYPE, self::PLACES_PARENTID);

		$this->check_raw_data($this->places, $row, $data, $fields);
	}

	private function check_raw_coords($row, $data) {
		$fields = array(self::COORDS_WOEID, self::COORDS_LAT, self::COORDS_LON,
			self::COORDS_NELAT, self::COORDS_NELON,
			self::COORDS_SWLAT, self::COORDS_SWLON);

		$this->check_raw_data($this->coords, $row, $data, $fields);
	}

	private function check_raw_changes($row, $data) {
		$fields = array(self::CHANGES_WOEID, self::CHANGES_REPID);

		$this->check_raw_data($this->changes, $row, $data, $fields);
	}

	private function check_raw_adjacencies($row, $data) {
		$fields = array(self::ADJACENCIES_WOEID, self::ADJACENCIES_NEIGHBOUR);

		$this->check_raw_data($this->adjacencies, $row, $data, $fields);
	}

	private function check_raw_aliases($row, $data) {
		$fields = array(self::ALIAS_WOEID, self::ALIAS_NAME, self::ALIAS_TYPE, self::ALIAS_LANG);

		$this->check_raw_data($this->aliases, $row, $data, $fields);
	}

	private function check_raw_admins($row, $data) {
		$fields = array(self::ADMINS_WOEID, self::ADMINS_ISO, self::ADMINS_STATE,
			self::ADMINS_COUNTY, self::ADMINS_LOCALADMIN, self::ADMINS_COUNTRY,
			self::ADMINS_CONTINENT);

		$this->check_raw_data($this->admins, $row, $data, $fields);
	}

	private function check_raw_data($file, $row, $data, $fields) {
		$missing = array();

		foreach ($fields as $field) {
			if (!isset($data[$field])) {
				$missing[] = $field;
			}
		}

		if (!empty($missing)) {
			$fields = implode(',',$missing);
			var_dump($data);
			throw new Exception("$file:$row - Missing fields $fields");
		}
	}
}

class BoundingBox {
	private $swlon;
	private $swlat;
	private $nelon;
	private $nelat;

	public function __construct($swlon, $swlat, $nelon, $nelat) {
		$this->swlon = (double) $swlon;
		$this->swlat = (double) $swlat;
		$this->nelon = (double) $nelon;
		$this->nelat = (double) $nelat;
	}

	public function is_empty() {
		return ($this->swlon == 0 && $this->swlat == 0 && $this->nelon == 0 && $this->nelat == 0);
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
			'type' => 'envelope',
			'coordinates' => array(
				array($east, $south),
				array($west, $north)

			)
		);
	}

	public function to_polygon() {
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
}

class MachineTag {
	private $placetypes;

	public function __construct() {
		$this->placetypes = new WoePlanetPlaceTypes();
	}

	public function placetype($woeid, $placetype) {
		$pt = $this->placetypes->get_by_id($placetype);

		return 'woeplanet:' . $pt['placetype']['tag'] . '=' . $woeid;
	}

	public function relationship($woeid, $rel) {
		return 'woeplanet:' . $rel . '=' . $woeid;
	}

	public function parse($tag) {
		$parts = explode('=', $tag);
		$key = explode(':', $parts[0]);
		$value = $parts[1];

		return array(
			'namespace' => $key[0],
			'predicate' => $key[1],
			'id' => $value
		);
	}
}

?>
