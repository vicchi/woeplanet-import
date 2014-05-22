#!/usr/bin/env php -q
<?php

require 'vendor/autoload.php';
require 'geoplanet-data-reader.php';

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
	private $version;
	private $provider;

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

	public function __construct($instance, $path, $verbose, $purge, $stage=NULL) {
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
			self::PLACETYPES_STAGE
		);

		foreach ($this->stages as $stage) {
			$path= 'geoplanet_' . $stage . '.sqlite3';
			$this->sqlite[$stage] = array(
				'path' => $path,
				'handle' => NULL
			);
		}

		$match = array();
		$pattern = '/geoplanet_data_([\d\.]+)$/';
		$ret = preg_match($pattern, basename($this->path), $match);
		$this->version = $match[1];
		$this->provider = sprintf('geoplanet %s', $this->version);

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
			$this->log("Missing $places");
			exit;
		}
	}

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

		$elapsed = $this->seconds_to_time($this->elapsed('run'));
		$this->log("Completed in $elapsed");
	}

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
			'index' => self::INDEX,
			'body' => array(
				'mappings' => array(
					self::PLACES_TYPE => array(
						'properties' => array(
							'centroid' => array(
								'type' => 'geo_point'
							),
							'bbox' => array(
								'type' => 'object',
								'properties' => array(
									'ne' => array(
										'type' => 'geo_point'
									),
									'sw' => array(
										'type' => 'geo_point'
									)
								)
							),
							'parent' => array(
								'type' => 'long'
							),
							'woeid' => array(
								'type' => 'long'
							),
							'woeid_adjacent' => array(
								'type' => 'long'
							)
						)
					),
					self::ADMINS_TYPE => array(
						'properties' => array(
							'woeid' => array(
								'type' => 'long'
							),
							'woeid_continent' => array(
								'type' => 'long'
							),
							'woeid_country' => array(
								'type' => 'long'
							),
							'woeid_county' => array(
								'type' => 'long'
							),
							'woeid_localadmin' => array(
								'type' => 'long'
							),
							'woeid_state' => array(
								'type' => 'long'
							)
						)
					)
				)
			)
		);

		$this->logVerbose("Creating index " . self::INDEX);
		$this->es->indices()->create($params);
		$elapsed = $this->seconds_to_time($this->elapsed('setup'));
		$this->log("Completed Elasticsearch index and type mappings in $elapsed");
	}

	private function index_places() {
		$this->elapsed('places');

		if ($this->sqlite[self::COORDS_STAGE]['handle'] === NULL) {
			$name = 'sqlite:' . $this->sqlite[self::COORDS_STAGE]['path'];
			$this->sqlite[self::COORDS_STAGE]['handle'] = new PDO($name);
		}
		$db = $this->sqlite[self::COORDS_STAGE]['handle'];
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

			$params = array(
				'body' => array(
					'woeid' => $data['WOE_ID'],
					'iso' => $data['ISO'],
					'name' => $data['Name'],
					'lang' => $data['Language'],
					'type' => $data['PlaceType'],
					'parent' => $data['Parent_ID'],
					'centroid' => array(
						'lat' => 0,
						'lon' => 0
					),
					'bbox' => array(
						'sw' => array(
							'lat' => 0,
							'lon' => 0
						),
						'ne' => array(
							'lat' => 0,
							'lon' => 0
						)
					),
					'provider_metadata' => $this->provider
				),
				'index' => self::INDEX,
				'type' => self::PLACES_TYPE,
				'id' => $data['WOE_ID'],
			);

			$statement = $db->prepare($select);
			$statement->bindParam(':woeid', $data['WOE_ID']);
			$statement->execute();
			$coords = $statement->fetch(PDO::FETCH_ASSOC);
			if ($coords) {
				$params['body']['centroid']['lat'] = $coords['lat'];
				$params['body']['centroid']['lon'] = $coords['lon'];
				$params['body']['bbox']['sw']['lat'] = $coords['swlat'];
				$params['body']['bbox']['sw']['lon'] = $coords['swlon'];
				$params['body']['bbox']['ne']['lat'] = $coords['nelat'];
				$params['body']['bbox']['ne']['lon'] = $coords['nelon'];
			}

			$old =$this->get_by_woeid($data['WOE_ID']);
			if ($old['found']) {
				if (isset($old['_source']['woeid_supersedes'])) {
					$params['body']['woeid_supersedes'] = $old['_source']['woeid_supersedes'];
				}
				if (isset($old['_source']['woeid_superseded_by'])) {
					$params['body']['woeid_superseded_by'] = $old['_source']['woeid_superseded_by'];
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

	private function index_coords() {
		$this->elapsed('coords');

		if ($this->sqlite[self::COORDS_STAGE]['handle'] === NULL) {
			$name = 'sqlite:' . $this->sqlite[self::COORDS_STAGE]['path'];
			$this->sqlite[self::COORDS_STAGE]['handle'] = new PDO($name);
		}
		$db = $this->sqlite[self::COORDS_STAGE]['handle'];
		$tsv = new GePplanetDataReader();

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

		$elapsed = $this->seconds_to_time($this->elapsed('coords'));
		$this->log("Completed indexing coordinates in $elapsed");
	}

	private function index_changes() {
		$this->elapsed('changes');

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

			$params = array(
				'body' => array(
					'doc' => array(
						'woeid' => $old_woeid,
						'woeid_superseded_by' => $new_woeid,
						'provider_metadata' => $this->provider
					),
					'doc_as_upsert' => true
				),
				'index' => self::INDEX,
				'type' => self::PLACES_TYPE,
				'id' => $old_woeid
			);

			$this->es->update($params);

			if ($new['found']) {
				$supersedes = array();
				if (isset($new['body']['woeid_supersedes'])) {
					$supersedes = $new['body']['woeid_superseded'];
				}
				if (array_search($old_woeid, $supersedes)== false) {
					$supersedes[] = $old_woeid;

					$params = array(
						'body' => array(
							'doc' => array(
								'woeid' => $new_woeid,
								'woeid_supersedes' => $supersedes,
								'provider_metadata' => $this->provider
							),
							'doc_as_upsert' => true
						),
						'index' => self::INDEX,
						'type' => self::PLACES_TYPE,
						'id' => $new_woeid
					);

					$this->es->update($params);
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

		$this->log("\nIndexed $row of $total changes");
		$elapsed = $this->seconds_to_time($this->elapsed('changes'));
		$this->log("Completed indexing changes in $elapsed");
	}

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

			$params = array(
				'body' => array(
					'doc' => array(
						'woeid' => $adj['woeid'],
						'woeid_adjacent' => $adjacent,
						'provider_metadata' => $this->provider
					),
					'doc_as_upsert' => true
				),
				'index' => self::INDEX,
				'type' => self::PLACES_TYPE,
				'id' => $adj['woeid']
			);

			$this->es->update($params);
			$this->show_status($row, $count);
		}

		$this->log("\nIndexed $row of $count adjacencies");

		$elapsed = $this->seconds_to_time($this->elapsed('adjacencies'));
		$this->log("Completed indexing adjacencies in $elapsed");
	}

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

		$setup = "CREATE TABLE geoplanet_aliases (
			woeid INTEGER,
			name TEXT,
			type TEXT
		)";
		$db->exec($setup);

		$setup = "CREATE INDEX aliases_by_woeid ON geoplanet_aliases (woeid)";
		$db->exec($setup);

		$row = 0;
		$insert = "INSERT INTO geoplanet_aliases(woeid, name, type) VALUES(:woeid,:name,:type)";
		$statement = $db->prepare($insert);

		while (($data = $tsv->get()) !== false) {
			$row++;
			$this->check_raw_aliases($row, $data);


			$statement->bindParam(':woeid', $data['WOE_ID']);
			$statement->bindParam(':name', $data['Name']);
			$statement->bindParam(':type', $data['Name_Type']);

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
				$key = sprintf('alias_%s', $type);

				$names = array();
				if (array_key_exists($key, $aliases)) {
					$names = $aliases[$key];
				}
				$names[] = $name;
				$aliases[$key] = $names;
			}

			$params = array(
				'body' => array(
					'doc' => array(
						'woeid' => $id,
						'provider_metadata' => $this->provider
					),
					'doc_as_upsert' => true
				),
				'index' => self::INDEX,
				'type' => self::PLACES_TYPE,
				'id' => $id
			);

			foreach ($aliases as $key => $value) {
				$params['body']['doc'][$key] = $value;
			}

			$this->es->update($params);
			$this->show_status($row, $total);
		}

		$this->log("\nIndexed $row of $count aliases");

		$elapsed = $this->seconds_to_time($this->elapsed('aliases'));
		$this->log("Completed indexing aliases in $elapsed");
	}

	private function index_admins() {
		$this->log("Indexing admins");
		$this->elapsed('admins');

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
					'woeid' => $data['WOE_ID'],
					'iso' => $data['ISO'],
					'woeid_state' => $data['State'],
					'woeid_county' => $data['County'],
					'woeid_localadmin' => $data['Local_Admin'],
					'woeid_country' => $data['Country'],
					'woeid_continent' => $data['Continent']
				),
				'index' => self::INDEX,
				'type' => self::ADMINS_TYPE,
				'id' => $data['WOE_ID']
			);
			$this->es->index($params);
			$this->show_status($row, $total);
		}

		$elapsed = $this->seconds_to_time($this->elapsed('admins'));
		$this->log("Completed indexing admins in $elapsed");
	}

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
			'placetypes' => null
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
		$fields = array(self::ALIAS_WOEID, self::ALIAS_NAME, self::ALIAS_TYPE);

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

?>
