#!/usr/bin/env php
<?php

require_once '../lib/runner.php';
require_once '../lib/timer.php';
require_once '../lib/placetypes.php';
require_once '../lib/reader.php';
require_once '../lib/geometry.php';

require_once 'vendor/autoload.php';

class GeoPlanetImport extends Woeplanet\Runner {
    const RUN_STAGE = 'run';
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

    private $purge = false;
    private $path = NULL;
    private $stage = NULL;
    private $snapshot = NULL;
    private $batchsize = 0;
    private $source;
    private $version;
    private $timestamp;
    private $history = array();

    private $sqlite;
    private $stages;
    private $timer;

    private $places;
    private $aliases;
    private $adjacencies;
    private $changes;
    private $coords;
    private $admins;

    private $files;
    private $placetypes;

    private $snapshot_repo_name;
    private $snapshot_repo_dir;

    public function __construct($server, $path, $verbose, $purge, $stage=NULL, $snapshot=NULL, $batchsize=0) {
        $client = new \Elasticsearch\Client();

        parent::__construct($server, $verbose);

        $this->placetypes = new \Woeplanet\PlaceTypes();

        $this->path = $path;
        $this->purge = $purge;

        if ($stage !== NULL) {
            $stage = strtolower($stage);
        }
        $this->stage = $stage;

        if ($snapshot !== NULL) {
            $this->snapshot = $snapshot;
        }

        if ($batchsize != 0) {
            $this->batchsize = $batchsize;
        }

        $this->sqlite = array();

        $this->stages = array(
            self::RUN_STAGE,
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

        $match = array();
        $pattern = '/(.+)_([\d\.]+)$/';
        $ret = preg_match($pattern, basename($this->path), $match);
        if ($ret === 1) {
            $this->source = 'GeoPlanet';
            $this->version = $match[2];
            $this->timestamp = time();
            $this->history[] = array(
                'source' => sprintf('%s %s', $this->source, $this->version),
                'timestamp' => (int) $this->timestamp
            );
        }

        else {
            $this->log('Can\'t get source and version from path ' . $this->path);
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

        $this->timer = new Woeplanet\Timer($this->stages);
        $this->snapshot_repo_dir = getcwd() . DIRECTORY_SEPARATOR . self::DATABASE . '-snapshots';
        $this->snapshot_repo_name = self::DATABASE . '-progress';

        foreach ($this->stages as $stage) {
            $path= 'geoplanet_' . $stage . '_' . $this->version . '.sqlite3';
            $this->sqlite[$stage] = array(
                'path' => $path,
                'handle' => NULL
            );
        }
    }

    public function run() {
        $this->timer->elapsed(self::RUN_STAGE);

        if (isset($this->stage)) {
            $this->timer->elapsed($this->stage);

            $func = "index_$this->stage";
            $this->$func();

            $elapsed = $this->timer->seconds_to_time($this->timer->elapsed($this->stage));
            $this->log("Completed stage $this->stage in $elapsed");
        }

        else {
            foreach ($this->stages as $stage) {
                if ($stage !== self::RUN_STAGE && $stage !== self::TEST_STAGE) {
                    $this->timer->elapsed($stage);

                    $func = "index_$stage";
                    $this->stage = $stage;
                    $this->$func();

                    $elapsed = $this->timer->seconds_to_time($this->timer->elapsed($stage));
                    $this->log("Completed stage $stage in $elapsed");
                }
            }
        }

        $elapsed = $this->timer->seconds_to_time($this->timer->elapsed(self::RUN_STAGE));
        $this->log("Completed in $elapsed");
    }

    private function index_setup() {
        $params = array(
            'index' => self::DATABASE
        );

        $this->logVerbose('Checking index ' . self::DATABASE);
        if ($this->client->indices()->exists($params)) {
            $this->logVerbose('Index ' . self::DATABASE . ' exists, skipping index and mappings creation');
        }

        else {
            $body = array(
                'mappings' =>array(
                    self::PLACES_TYPE => array(
                        '_timestamp' => array(
                            'enabled' => true
                        ),
                        'dynamic' => 'strict',
                        'properties' => array(
                            'woe:id' => array(
                                'type' => 'long'
                            ),
                            'iso' => array(
                                'type' => 'string'
                            ),
                            'name' => array(
                                'type' => 'string'
                            ),
                            'lang' => array(
                                'type' => 'string'
                            ),
                            'woe:placetype' => array(
                                'type' => 'long'
                            ),
                            'woe:placetypename' => array(
                                'type' => 'string'
                            ),
                            'woe:parent' => array(
                                'type' => 'long'
                            ),
                            'history' => array(
                                'type' => 'nested',
                                'properties' => array(
                                    'source' => array(
                                        'type' => 'string'
                                    ),
                                    'timestamp' => array(
                                        'type' => 'long'
                                    )
                                )
                            ),
                            'woe:centroid' => array(
                                'type' => 'geo_point'
                            ),
                            'woe:bounds' => array(
                                'type' => 'geo_shape'
                            ),
                            'woe:adjacent' => array(
                                'type' => 'string'
                            ),
                            'woe:alias_q' => array(
                                'type' => 'nested',
                                'properties' => array(
                                    'lang' => array(
                                        'type' => 'string'
                                    ),
                                    'name' => array(
                                        'type' => 'string'
                                    )
                                )
                            ),
                            'woe:alias_v' => array(
                                'type' => 'nested',
                                'properties' => array(
                                    'lang' => array(
                                        'type' => 'string'
                                    ),
                                    'name' => array(
                                        'type' => 'string'
                                    )
                                )
                            ),
                            'woe:alias_a' => array(
                                'type' => 'nested',
                                'properties' => array(
                                    'lang' => array(
                                        'type' => 'string'
                                    ),
                                    'name' => array(
                                        'type' => 'string'
                                    )
                                )
                            ),
                            'woe:alias_s' => array(
                                'type' => 'nested',
                                'properties' => array(
                                    'lang' => array(
                                        'type' => 'string'
                                    ),
                                    'name' => array(
                                        'type' => 'string'
                                    )
                                )
                            ),
                            'woe:alias_p' => array(
                                'type' => 'nested',
                                'properties' => array(
                                    'lang' => array(
                                        'type' => 'string'
                                    ),
                                    'name' => array(
                                        'type' => 'string'
                                    )
                                )
                            ),
                            'woe:state' => array(
                                'type' => 'string'
                            ),
                            'woe:county' => array(
                                'type' => 'string'
                            ),
                            'woe:localadmin' => array(
                                'type' => 'string'
                            ),
                            'woe:country' => array(
                                'type' => 'string'
                            ),
                            'woe:continent' => array(
                                'type' => 'string'
                            )
                        )
                    ),
                    self::PLACETYPES_TYPE => array(
                        '_timestamp' => array(
                            'enabled' => true
                        ),
                        'dynamic' => 'strict',
                        'properties' => array(
                            'history' => array(
                                'type' => 'nested',
                                'properties' => array(
                                    'source' => array(
                                        'type' => 'string'
                                    ),
                                    'timestamp' => array(
                                        'type' => 'long'
                                    )
                                )
                            )
                        )
                    )
                )
            );

            $params = array(
                'index' => self::DATABASE,
                'body' => $body
            );

            $this->logVerbose('Creating index ' . self::DATABASE . ' and mappings');
            $this->client->indices()->create($params);
            $this->setup_snapshots();
        }
    }

    private function index_places() {
        $reader = new \Woeplanet\Reader();
        $reader->open($this->files[$this->places]);
        $total = $reader->size();
        $row = 0;
        $batch = 0;
        $max_woeid = 0;
        $params = array();

        $this->log("Indexing $total places");

        while (($data = $reader->get()) !== false) {
            $row++;
            $this->check_raw_places($row, $data);
            $old = $this->get_woeid($data['WOE_ID']);
            $placetype = $this->placetypes->get_by_shortname($data['PlaceType']);
            if (!$placetype['found']) {
                throw new Exception('Cannot find match for placetype ' . $data['PlaceType'] . ' for WOEID ' . $data['WOE_ID']);
            }

            $doc = array(
                // '_id' => (int)$data['WOE_ID'],
                'woe:id' => (int)$data['WOE_ID'],
                'iso' => $data['ISO'],
                'name' => $data['Name'],
                'lang' => $data['Language'],
                //'woe:centroid' => array(-0.12714, 51.506321),
                // 'woe:bbox' => array(
                //     array(-0.563, 51.261318),
                //     array(0.28036, 51.686031)
                // ),
                // 'woe:bounds' => array(
                //     'type' => 'Polygon',
                //     'coordinates' => array(
                //         array(
                //             array(-0.563, 51.261),
                //             array(-0.563, 51.686),
                //             array(0.28, 51.686),
                //             array(0.28, 51.261),
                //             array(-0.563, 51.261)
                //         )
                //     )
                // ),
                'woe:placetype' => (int)$placetype['placetype']['id'],
                'woe:placetypename' => $placetype['placetype']['name'],
                'woe:parent' => (int)$data['Parent_ID'],
                //'woe:supercedes' => 1234,
                //'woe:superceded' => 1234,
                'history' => $this->history
                // )
                // 'index' => self::DATABASE,
                // 'type' => self::PLACES_TYPE,
                // 'id' => $data['WOE_ID']
            );

            if (!empty($old)) {
                $history = array();
                if (isset($old['history'])) {
                    $history = $old['history'];
                }
                $history[] = $this->history[0];
                $doc['history']= $history;

                if (isset($old['woe:supersedes'])) {
                    $doc['woe:supersedes'] = $old['woe:supersedes'];
                }

                if (isset($old['woe:superseded'])) {
                    $doc['woe:superseded'] = $old['woe:superseded'];
                }
            }

            $params['body'][] = array(
                'index' => array(
                    '_index' => self::DATABASE,
                    '_type' => self::PLACES_TYPE,
                    '_id' => $data['WOE_ID']
                )
            );
            $params['body'][] = $doc;

            if (++$batch > $this->batchsize) {
                error_log("Bulk indexing batch of $this->batchsize documents");
                $ret = $this->client->bulk($params);
                $params = array();
                error_log("Bulk index of $this->batchsize documents completed");
                unset($ret);
                $batch = 0;
            }

            $this->show_status($row, $total);
            if ($max_woeid <= $data['WOE_ID']) {
                $max_woeid = $data['WOE_ID'];
            }
        }

        if (!empty($params)) {
            error_log("Bulk indexing final batch of " . count($params['body']) . " documents");
            $ret = $this->client->bulk($params);
            unset($ret);
        }

        $doc = array(
            'max_woeid' => (int)$max_woeid
        );
        $params = array(
            'body' => $doc,
            'index' => self::DATABASE,
            'type' => self::META_TYPE,
            'id' => 1
        );
        $this->client->index($params);

        $this->create_snapshot();
        $this->log("\nFinished indexing $total places");
    }

    private function index_coords() {
        if (false !== array_key_exists($this->coords, $this->files)) {
            $reader = new \Woeplanet\Reader();
            $reader->open($this->files[$this->coords]);
            $total = $reader->size();
            $row = 0;
            $batch = 0;
            $params = array();

            $this->log("Indexing $total coords");

            while (($data = $reader->get()) !== false) {
                $row++;
                $this->check_raw_coords($row, $data);

                $lat = $this->sanitize_coord($data['Lat']);
                $lon = $this->sanitize_coord($data['Lon']);

                $nelat = $this->sanitize_coord($data['NE_Lat']);
                $nelon = $this->sanitize_coord($data['NE_Lon']);

                $swlat = $this->sanitize_coord($data['SW_Lat']);
                $swlon = $this->sanitize_coord($data['SW_Lon']);

                $doc = $this->get_woeid($data['WOE_ID']);

                $centroid = new \Woeplanet\Centroid($lon, $lat);
                if (!$centroid->is_empty() && $centroid->is_valid()) {
                    $doc['woe:centroid'] = $centroid->to_json();
                }
                else {
                    error_log('Centroid sanity check failed for WOEID ' . $data['WOE_ID'] . ', skipping');
                    error_log($centroid);
                }

                $bbox = new \Woeplanet\BoundingBox($swlon, $swlat, $nelon, $nelat);
                if ($bbox->is_empty()) {
                    error_log('Bounding box for WOEID ' . $data['WOE_ID'] . ', is empty skipping');
                    error_log($bbox);
                }
                else if (!$bbox->is_valid()) {
                    error_log('Bounding box for WOEID ' . $data['WOE_ID'] . ', is invalid skipping');
                    error_log($bbox);
                }

                else {
                    $doc['woe:bounds'] = $bbox->to_json();
                }

                $params = array(
                    'body' => $doc,
                    'index' => self::DATABASE,
                    'type' => self::PLACES_TYPE,
                    'id' => $data['WOE_ID']
                );
                $this->client->index($params);

                // $params['body'][] = array(
                //     'index' => array(
                //         '_index' => self::DATABASE,
                //         '_type' => self::PLACES_TYPE,
                //         '_id' => $data['WOE_ID']
                //     )
                // );
                // $params['body'][] = $doc;

                // if (++$batch > $this->batchsize) {
                //     error_log("Bulk indexing batch of $this->batchsize documents");
                //     $ret = $this->client->bulk($params);
                //     $params = array();
                //     error_log("Bulk index of $this->batchsize documents completed");
                //     unset($ret);
                //     $batch = 0;
                // }

                $this->show_status($row, $total);
            }

            if (!empty($params)) {
                error_log("Bulk indexing final batch of " . count($params['body']) . " documents");
                $ret = $this->client->bulk($params);
                unset($ret);
            }

            $this->create_snapshot();
            $this->log("\nFinished indexing $total coords");
        }

        else {
            $this->log("No coords found for $this->coords; skipping");
        }
    }

    private function index_changes() {
        if (false !== array_key_exists($this->changes, $this->files)) {
            $reader = new \Woeplanet\Reader();
            $reader->open($this->files[$this->changes]);
            $total = $reader->size();
            $row = 0;

            $this->log("Indexing $total changes");

            while (($data = $reader->get()) !== false) {
                $row++;
                $this->check_raw_changes($row, $data);

                // Trap empty or malformed change lines (v7.8.1 are you feeling guilty?)
                if ((intval($data['Woe_id']) == 0) || (intval($data['Rep_id']) == 0)) {
                    error_log("Invalid or empty change values near line $row");
                    continue;
                }

                // Hey, let's be consistent in our column naming convention ... WTF
                $old_woeid = $data['Woe_id'];
                $new_woeid = $data['Rep_id'];

                $old = $this->get_woeid($old_woeid);
                $new = $this->get_woeid($new_woeid);

                if (NULL !== $new) {
                    $supersedes = array();
                    if (isset($new['woe:supersedes'])) {
                        $supersedes = $new['woe:supersedes'];
                    }
                    if (array_search($old_woeid, $supersedes) == false) {
                        $supersedes[] = $old_woeid;
                        $new['woe:supersedes'] = $supersedes;

                        $params = array(
                            'body' => $new,
                            'index' => self::DATABASE,
                            'type' => self::PLACES_TYPE,
                            'id' => $new_woeid
                        );
                        $this->client->index($params);
                    }
                }

                else {
                    error_log("WTF ... no record found for new WOEID $new_woeid, creating empty placeholder");

                    $doc = $this->make_placeholder_woeid($new_woeid);
                    $doc['woe:supercedes'] = array($old_woeid);

                    $params = array(
                        'body' => $doc,
                        'index' => self::DATABASE,
                        'type' => self::PLACES_TYPE,
                        'id' => $new_woeid
                    );
                    $this->client->index($params);

                    $this->refresh_meta($new_woeid);
                    $new = $this->get_woeid($new_woeid);
                }

                if (NULL !== $old) {
                    $doc = $old;
                    $doc['woe:superseded'] = $new_woeid;

                    $params = array(
                        'body' => $doc,
                        'index' => self::DATABASE,
                        'type' => self::PLACES_TYPE,
                        'id' => $old_woeid
                    );
                    $this->client->index($params);
                }
                else {
                    error_log("Oops ... no document found for old WOEID $old_woeid; back-filled with $new_woeid");

                    $doc = $this->make_placeholder_woeid($old_woeid, $new);
                    $doc['woe:superseded'] = $new_woeid;
                    $doc['history'] = $this->history;

                    $params = array(
                        'body' => $doc,
                        'index' => self::DATABASE,
                        'type' => self::PLACES_TYPE,
                        'id' => $old_woeid
                    );
                    $this->client->index($params);
                    $this->refresh_meta($new_woeid);
                }
                $this->show_status($row, $total);
            }

            $this->create_snapshot();
            $this->log("\nFinished indexing $total changes");
        }
        else {
            $this->log("No changes found for $this->changes; skipping");
        }
    }

    private function index_adjacencies() {
        $pre_cached = false;
        if ($this->sqlite[self::ADJACENCIES_STAGE]['handle'] === NULL) {
            $file = $this->sqlite[self::ADJACENCIES_STAGE]['path'];
            $name = 'sqlite:' . $file;
            if (file_exists($file)) {
                $this->logVerbose("Cache $file already exists, using this cache for adjacencies");
                $pre_cached = true;
            }
            $this->sqlite[self::ADJACENCIES_STAGE]['handle'] = new PDO($name);
        }

        $db = $this->sqlite[self::ADJACENCIES_STAGE]['handle'];
        $this->init_cache($db);
        $reader = new \Woeplanet\Reader();
        $reader->open($this->files[$this->adjacencies]);
        $total = $reader->size();

        if (!$pre_cached) {
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

            while (($data = $reader->get()) !== false) {
                $row++;
                $this->check_raw_adjacencies($row, $data);

                $statement->bindParam(':woeid', $data['Place_WOE_ID']);
                $statement->bindParam(':neighbour', $data['Neighbour_WOE_ID']);

                $statement->execute();

                $this->show_status($row, $total);
            }

            $this->logVerbose("\nCached $row of $total adjacencies");
        }

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
                $doc = $this->get_woeid($neighbour['neighbour']);
                if (NULL === $doc) {
                    $doc = $this->make_placeholder_woeid($neighbour['neighbour']);
                    error_log("WTF ... no record found for neighbour WOEID " . $neighbour['neighbour'] . ", creating empty placeholder");

                    $params = array(
                        'body' => $doc,
                        'index' => self::DATABASE,
                        'type' => self::PLACES_TYPE,
                        'id' => $neighbour['neighbour']
                    );
                    $this->client->index($params);
                    $doc = $this->get_woeid($neighbour['neighbour']);
                    $this->refresh_meta($neighbour['neighbour']);
                }

                $placecode = $doc['woe:placetype'];
                $placeentry = $this->placetypes->get_by_id($placecode);
                if (!$placeentry['found']) {
                    throw new Exception('Cannot find match for placetype id ' . $placecode . ' for WOEID ' . $neighbour['neighbour']);
                }

                $placetag = $placeentry['placetype']['tag'];
                $adjacent[] = sprintf('woe:%s=%s', $placetag, $neighbour['neighbour']);
            }

            $doc = $this->get_woeid($adj['woeid']);

            if (NULL === $doc) {
                $doc = $this->make_placeholder_woeid($adj['woeid']);
                error_log("WTF ... no record found for adjacent source WOEID " . $adj['woeid'] . ", creating empty placeholder");

                $params = array(
                    'body' => $doc,
                    'index' => self::DATABASE,
                    'type' => self::PLACES_TYPE,
                    'id' => $adj['woeid']
                );
                $this->client->index($params);

                $doc = $this->get_woeid($adj['woeid']);
                $this->refresh_meta($adj['woeid']);
            }

            $doc['woe:adjacent'] = $adjacent;
            $params = array(
                'body' => $doc,
                'index' => self::DATABASE,
                'type' => self::PLACES_TYPE,
                'id' => $doc['woe:id']
            );
            $this->client->index($params);
            $this->show_status($row, $count);
        }

        $this->create_snapshot();
        $this->log("\nFinished indexing $row of $count adjacencies");
    }

    private function index_aliases() {
        $pre_cached = false;
        if ($this->sqlite[self::ALIASES_STAGE]['handle'] === NULL) {
            $file = $this->sqlite[self::ALIASES_STAGE]['path'];
            $name = 'sqlite:' . $file;
            if (file_exists($file)) {
                $this->logVerbose("Cache $file already exists, using this cache for adjacencies");
                $pre_cached = true;
            }
            $this->sqlite[self::ALIASES_STAGE]['handle'] = new PDO($name);
        }

        $db = $this->sqlite[self::ALIASES_STAGE]['handle'];
        $this->init_cache($db);
        $reader = new \Woeplanet\Reader();
        $reader->open($this->files[$this->aliases]);
        $total = $reader->size();

        if (!$pre_cached) {
            $this->log("Caching and aggregating " . $total . " aliases");

            $setup = "create table if not exists geoplanet_aliases (woeid INTEGER, name TEXT, type TEXT, lang TEXT);";
            $db->exec($setup);

            $setup = "CREATE INDEX aliases_by_woeid ON geoplanet_aliases(woeid);";
            $db->exec($setup);

            $row = 0;
            $insert = "INSERT INTO geoplanet_aliases(woeid, name, type, lang) VALUES(:woeid,:name,:type,:lang)";
            $statement = $db->prepare($insert);

            while (($data = $reader->get()) !== false) {
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
        }

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
            $q = array();
            $v = array();
            $a = array();
            $s = array();
            $p = array();

            while (($alias = $statement->fetch(PDO::FETCH_ASSOC)) !== false) {
                // Name Types: https://developer.yahoo.com/forum/GeoPlanet-General-Discussion/what-39-s-name-type-field-in-geoplanet-aliases-7-2-tsv/1245492689000-1cda9941-6417-3875-9590-c3dc4335c663/
                // And also: http://www.aaronland.info/weblog/2009/12/21/redacted/#woelr
                // P (preferred name):
                // Q (qualified name): this name is the preferred name for the place in a language different than that used by residents of the place (e.g. "紐約" for New York)
                // V (variation): this name is a well-known (but unofficial) name for the place (e.g. "New York City" for New York)
                // A (abbreviation): this name is a abbreviation or code for the place (e.g. "NYC" for New York)
                // S (synonym): this name is a colloquial name for the place (e.g. "Big Apple" for New York)

                $woeid = $alias['woeid'];
                $name = $alias['name'];
                $type = $alias['type'];
                $lang = $alias['lang'];
                // woe:alias_UNK_V
                // $key = sprintf('woe:alias_%s_%s', $lang, $type);
                //
                // $names = array();
                // if (array_key_exists($key, $aliases)) {
                //     $names = $aliases[$key];
                // }
                //
                // if (!in_array($name, $names, true)) {
                //     $names[] = $name;
                // }
                // $aliases[$key] = $names;

                $element = array('lang' => $lang, 'name' => $name);
                switch ($type) {
                    case 'Q': $q[] = $element; break;
                    case 'V': $v[] = $element; break;
                    case 'A': $a[] = $element; break;
                    case 'S': $s[] = $element; break;
                    case 'P': $p[] = $element; break;
                }
            }

            $doc = $this->get_woeid($id);

            if (!empty($q)) {
                $doc['woe:alias_q'] = $q;
            }
            if (!empty($v)) {
                $doc['woe:alias_v'] = $q;
            }
            if (!empty($a)) {
                $doc['woe:alias_a'] = $q;
            }
            if (!empty($s)) {
                $doc['woe:alias_s'] = $q;
            }
            if (!empty($p)) {
                $doc['woe:alias_p'] = $p;
            }

            // foreach ($aliases as $key => $value) {
            //     $doc[$key] = $value;
            // }

            $params = array(
                'body' => $doc,
                'index' => self::DATABASE,
                'type' => self::PLACES_TYPE,
                'id' => $id
            );
            $this->client->index($params);
            $this->show_status($row, $total);
        }

        $this->log("\nFinished indexing $total aliases");
        $this->create_snapshot();
    }

    private function index_admins() {
        if (false !== array_key_exists($this->admins, $this->files)) {
            $reader = new \Woeplanet\Reader();
            $reader->open($this->files[$this->admins]);
            $total = $reader->size();

            $this->log("Indexing $total admins");
            $row = 0;

            while(($data = $reader->get()) !== false) {
                $row++;
                $this->check_raw_admins($row, $data);

                $doc = $this->get_woeid($data['WOE_ID']);
                if (!empty($data['State'])) {
                    $doc['woe:state'] = $data['State'];
                }
                if (!empty($data['County'])) {
                    $doc['woe:county'] = $data['County'];
                }
                if (!empty($data['Local_Admin'])) {
                    $doc['woe:localadmin'] = $data['Local_Admin'];
                }
                if (!empty($data['Country'])) {
                    $doc['woe:country'] = $data['Country'];
                }
                if (!empty($data['Continent'])) {
                    $doc['woe:continent'] = $data['Continent'];
                }

                $params = array(
                    'body' => $doc,
                    'index' => self::DATABASE,
                    'type' => self::PLACES_TYPE,
                    'id' => $data['WOE_ID']
                );
                $this->client->index($params);
                $this->show_status($row, $total);
            }

            $this->log("\nFinished indexing $total admins");
            $this->make_snapshot();
        }

        else {
            $this->log("No admins found for $this->admins; skipping");
        }
    }

    private function index_placetypes() {
        $placetypes = $this->placetypes->get();
        $total = count($placetypes);
        $row = 0;
        $this->log("Indexing $total placetypes");

        foreach ($placetypes as $placetype) {
            $row++;
            $doc = $placetype;

            $params = array(
                'body' => $doc,
                'index' => self::DATABASE,
                'type' => self::PLACETYPES_TYPE,
                'id' => $placetype['id']
            );
            $this->client->index($params);

            $this->show_status($row, $total);
        }

        $this->log("\nFinished indexing $total placetypes");
        $this->create_snapshot();
    }

    private function index_test() {
        $this->setup_snapshots();
        $this->create_snapshot();
        return;

        $params = array(
            'index' => self::DATABASE
        );
        if ($this->client->indices()->exists($params)) {
            $this->client->indices()->delete($params);
        }
        $body = array(
            'mappings' =>array(
                self::PLACES_TYPE => array(
                    '_timestamp' => array(
                        'enabled' => true
                    ),
                    'dynamic' => 'strict',
                    'properties' => array(
                        'iso' => array(
                            'type' => 'string'
                        ),
                        'name' => array(
                            'type' => 'string'
                        ),
                        'lang' => array(
                            'type' => 'string'
                        ),
                        'woe:placetype' => array(
                            'type' => 'long'
                        ),
                        'woe:placetypename' => array(
                            'type' => 'string'
                        ),
                        'woe:parent' => array(
                            'type' => 'long'
                        ),
                        'history' => array(
                            'type' => 'nested',
                            'properties' => array(
                                'source' => array(
                                    'type' => 'string'
                                ),
                                'timestamp' => array(
                                    'type' => 'long'
                                )
                            )
                        ),
                        // 'woe:centroid' => array(
                        //     'properties' => array(
                        //         'coordinates' => array(
                        //             'type' => 'geo_shape'
                        //         )
                        //     )
                        // ),
                        'woe:centroid' => array(
                            'type' => 'geo_point'
                        ),
                        // 'woe:bounds' => array(
                        //     'properties' => array(
                        //         'coordinates' => array(
                        //             'type' => 'geo_shape'
                        //         )
                        //     )
                        // ),
                        'woe:bounds' => array(
                            'type' => 'geo_shape'
                        ),
                        'woe:adjacent' => array(
                            'type' => 'string'
                        ),
                        'woe:alias_q' => array(
                            'type' => 'nested',
                            'properties' => array(
                                'lang' => array(
                                    'type' => 'string'
                                ),
                                'name' => array(
                                    'type' => 'string'
                                )
                            )
                        ),
                        'woe:alias_v' => array(
                            'type' => 'nested',
                            'properties' => array(
                                'lang' => array(
                                    'type' => 'string'
                                ),
                                'name' => array(
                                    'type' => 'string'
                                )
                            )
                        ),
                        'woe:alias_a' => array(
                            'type' => 'nested',
                            'properties' => array(
                                'lang' => array(
                                    'type' => 'string'
                                ),
                                'name' => array(
                                    'type' => 'string'
                                )
                            )
                        ),
                        'woe:alias_s' => array(
                            'type' => 'nested',
                            'properties' => array(
                                'lang' => array(
                                    'type' => 'string'
                                ),
                                'name' => array(
                                    'type' => 'string'
                                )
                            )
                        ),
                        'woe:alias_p' => array(
                            'type' => 'nested',
                            'properties' => array(
                                'lang' => array(
                                    'type' => 'string'
                                ),
                                'name' => array(
                                    'type' => 'string'
                                )
                            )
                        ),
                        'woe:state' => array(
                            'type' => 'string'
                        ),
                        'woe:county' => array(
                            'type' => 'string'
                        ),
                        'woe:localadmin' => array(
                            'type' => 'string'
                        ),
                        'woe:country' => array(
                            'type' => 'string'
                        ),
                        'woe:continent' => array(
                            'type' => 'string'
                        )
                    )
                ),
                self::PLACETYPES_TYPE => array(
                    '_timestamp' => array(
                        'enabled' => true
                    ),
                    'properties' => array(
                        'history' => array(
                            'type' => 'nested',
                            'properties' => array(
                                'source' => array(
                                    'type' => 'string'
                                ),
                                'timestamp' => array(
                                    'type' => 'long'
                                )
                            )
                        )
                    )
                )
            )
        );

        $params = array(
            'index' => self::DATABASE,
            'body' => $body
        );

        $this->logVerbose('Creating index ' . self::DATABASE . ' and mappings');
        $this->client->indices()->create($params);

        $doc = array(
            "_id" => 44418,
            // "woe:id" => NumberLong(44418),
            "iso" => "GB",
            "name" => "London",
            "lang" => "ENG",
            "woe:placetype" => 7,
            "woe:placetypename" => "Town",
            "woe:parent" => 23416974,
            "history" => array(
                array(
                    "source" => "GeoPlanet 7.10.0",
                    "timestamp" => 1432286437
                )
            ),
            "woe:centroid" => array(-0.12714000000000000301, 51.506320999999999799),
            // // "woe:bbox" =>[
            // //   [
            // //     0.2803599999999999981,
            // //     51.261318000000002826
            // //   ],
            // //   [
            // //     -0.56299999999999994493,
            // //     51.686030999999999835
            // //   ]
            // // ],
            "woe:bounds" => array(
                "type" => "polygon",
                "coordinates" => array(
                    array(
                        array(-0.56299999999999994493, 51.261318000000002826),
                        array(-0.56299999999999994493, 51.686030999999999835),
                        array(0.2803599999999999981, 51.686030999999999835),
                        array(0.2803599999999999981, 51.261318000000002826),
                        array(-0.56299999999999994493, 51.261318000000002826)
                    )
                )
            ),
            "woe:adjacent" => array(
                "woe:town=18074",
                "woe:town=19919",
                "woe:town=19551",
                "woe:town=14482",
                "woe:town=15786",
                "woe:town=11448",
                "woe:town=35986",
                "woe:town=31287",
                "woe:town=30600",
                "woe:town=24596",
                "woe:town=39684",
                "woe:town=26662",
                "woe:town=23839",
                "woe:town=37214",
                "woe:town=39437",
                "woe:town=15824",
                "woe:town=25681",
                "woe:town=21746",
                "woe:town=12052",
                "woe:town=28278",
                "woe:town=18076",
                "woe:town=35337",
                "woe:town=17535",
                "woe:town=17333",
                "woe:town=17601",
                "woe:town=22218",
                "woe:town=21298",
                "woe:town=11566",
                "woe:town=39690",
                "woe:town=29718",
                "woe:town=37013",
                "woe:town=14729",
                "woe:town=39185",
                "woe:town=20094177",
                "woe:town=56064358",
                "woe:town=26712",
                "woe:town=23517",
                "woe:town=29719",
                "woe:town=28976",
                "woe:town=15566",
                "woe:town=40561",
                "woe:town=25556",
                "woe:town=22552",
                "woe:town=16174",
                "woe:town=21490",
                "woe:town=28835375",
                "woe:town=13267",
                "woe:town=39912",
                "woe:town=15890",
                "woe:town=14384",
                "woe:suburb=38838",
                "woe:town=14728",
                "woe:town=39321",
                "woe:town=35295",
                "woe:town=27508",
                "woe:town=13781",
                "woe:town=39070",
                "woe:town=35009",
                "woe:suburb=17002",
                "woe:town=34397",
                "woe:town=36880",
                "woe:town=39072",
                "woe:town=37149",
                "woe:town=36749",
                "woe:suburb=31092",
                "woe:town=11791",
                "woe:town=17953",
                "woe:town=21536",
                "woe:town=27397",
                "woe:town=11209",
                "woe:town=19055",
                "woe:town=33295",
                "woe:town=36612",
                "woe:town=19643",
                "woe:town=39188",
                "woe:suburb=33508",
                "woe:town=24595",
                "woe:town=32682",
                "woe:town=19649",
                "woe:town=25985",
                "woe:town=34560",
                "woe:town=39747",
                "woe:suburb=38594",
                "woe:town=11157",
                "woe:town=15405",
                "woe:town=19688",
                "woe:town=32485",
                "woe:town=40385"
            ),
            "woe:alias_q" => array(
                array("lang" =>"DUT", "name" =>"Londen"),
                array("lang" =>"FRE", "name" =>"Londres"),
                array("lang" =>"ITA", "name" =>"Londra"),
                array(
                    "lang" =>"POR",
                    "name" =>"Londres"
                ),
                array(
                    "lang" =>"SPA",
                    "name" =>"Londres"
                ),
                array(
                    "lang" =>"JPN",
                    "name" =>"ロンドン"
                ),
                array(
                    "lang" =>"FIN",
                    "name" =>"Lontoo"
                ),
                array(
                    "lang" =>"KOR",
                    "name" =>"런던"
                ),
                array(
                    "lang" =>"CZE",
                    "name" =>"Londýn"
                ),
                array(
                    "lang" =>"POL",
                    "name" =>"Londyn"
                ),
                array(
                    "lang" =>"RUM",
                    "name" =>"Londra"
                ),
                array(
                    "lang" =>"CHI",
                    "name" =>"倫敦"
                )
            ),
            "woe:alias_v" => array(
                array(
                    "lang" =>"DUT",
                    "name" =>"Londen"
                ),
                array(
                    "lang" =>"FRE",
                    "name" =>"Londres"
                ),
                array(
                    "lang" =>"ITA",
                    "name" =>"Londra"
                ),
                array(
                    "lang" =>"POR",
                    "name" =>"Londres"
                ),
                array(
                    "lang" =>"SPA",
                    "name" =>"Londres"
                ),
                array(
                    "lang" =>"JPN",
                    "name" =>"ロンドン"
                ),
                array(
                    "lang" =>"FIN",
                    "name" =>"Lontoo"
                ),
                array(
                    "lang" =>"KOR",
                    "name" =>"런던"
                ),
                array(
                    "lang" =>"CZE",
                    "name" =>"Londýn"
                ),
                array(
                    "lang" =>"POL",
                    "name" =>"Londyn"
                ),
                array(
                    "lang" =>"RUM",
                    "name" =>"Londra"
                ),
                array(
                    "lang" =>"CHI",
                    "name" =>"倫敦"
                )
            ),
            "woe:state" =>"24554868",
            "woe:county" =>"23416974",
            "woe:country" =>"23424975",
            "woe:continent" =>"24865675"
        );

        error_log(json_encode($doc, JSON_PRETTY_PRINT));

        $params = array(
            'body' => $doc,
            'index' => self::DATABASE,
            'type' => self::PLACES_TYPE,
            'id' => 44418
        );

        $ret = $this->client->index($params);
        error_log(var_export($ret, true));

        $doc = $this->get_woeid(44418);
        // error_log(var_export($doc, true));
        $doc = $this->get_woeid(44417);

        // $bbox = new \Woeplanet\BoundingBox(-1.51576, 51.414101, -1.51631, 51.414101);
        // echo $bbox;
        // if ($bbox->is_empty()) {
        //     error_log('empty');
        // }
        // else {
        //     error_log('not empty');
        // }
        // if ($bbox->is_valid()) {
        //     error_log('valid');
        // }
        // else {
        //     error_log('invalid');
        // }
        //
        // $json = json_encode($bbox->to_geojson());
        // error_log($json);
        // $bounds = geoPHP::load($json, 'json');
        // if ($bounds->isClosed()) {
        //     error_log('is closed');
        // }
        // error_log('points: ' . $bounds->numPoints());
        // exit;

        // $timeout = 480000;
        // $options = array(
        //     'socketTimeoutMS' => -1,
        //     'maxTimeMS' => $timeout,
        //     'background' => true
        // );
        // $this->log("Defining geospatial indices: woe:centroid");
        // $this->collections[self::PLACES_COLLECTION]->createIndex(array('woe:centroid' => '2dsphere'), $options);
        // $this->log("Defining geospatial indices: woe:bounds");
        // $this->collections[self::PLACES_COLLECTION]->createIndex(array('woe:bounds' => '2dsphere'), $options);

        // $this->make_snapshot();

        // $params = array(
        //     'body' => array(
        //         self::ADMINS_TYPE => array(
        //             '_timestamp' => array(
        //                 'enabled' => true
        //             ),
        //             'properties' => array(
        //                 'woe:id' => array(
        //                     'type' => 'long'
        //                 ),
        //                 'woe:continent' => array(
        //                     'type' => 'long'
        //                 ),
        //                 'woe:country' => array(
        //                     'type' => 'long'
        //                 ),
        //                 'woe:county' => array(
        //                     'type' => 'long'
        //                 ),
        //                 'woe:local-admin' => array(
        //                     'type' => 'long'
        //                 ),
        //                 'woe:state' => array(
        //                     'type' => 'long'
        //                 ),
        //                 'history' => array(
        //                     'properties' => array(
        //                         'source' => array(
        //                             'type' => 'string'
        //                         ),
        //                         'timestamp' => array(
        //                             'type' => 'long'
        //                         )
        //                     )
        //                 )
        //             )
        //         )
        //     ),
        //     'index' => self::DATABASE,
        //     'type' => self::ADMINS_TYPE
        // );
        // $this->es->indices()->putMapping($params);
        // exit;

        // $woeid = 44418;
        // $admins = array();
        // $admins = $this->collect_admins($woeid, $admins);
        // error_log("Final admins: " . var_export($admins, true));
        // exit;

        // $meta = $this->get_meta();
        // error_log(var_export($meta, true));
        // exit;

        // $snapshot = $this->history[0]['source'] . ' places';
        // $this->create_snapshot($snapshot);
        // $this->log("\nFinished indexing $total places");
        //
        // exit;

        // $this->snapshot_repo_name;
        // $this->snapshot_repo_dir;
        //
        // $path = getcwd();
        // $repodir = $path . DIRECTORY_SEPARATOR . self::DATABASE . '-snapshots';
        //
        // $repo = self::DATABASE . '-progress';
        //
        // $params = array(
        //     'repository' => $repo . 'nothing'
        // );
        // $doc = $this->es->snapshot()->getRepository($params);
        // $this->log(var_export($doc));
        //
        // exit;


        // $params = array(
        //     'repository' => $repo,
        //     'snapshot' => 'test-me',
        //     'wait_for_completion' => true
        // );
        // $json = json_encode($params, JSON_PRETTY_PRINT);
        // $this->log($json);
        // $this->es->snapshot()->create($params);
        // exit;


        // $params = array(
        //     'repository' => $repo,
        //     'body' => array(
        //         'type' => 'fs',
        //         'settings' => array(
        //             'compress' => true,
        //             'location' => $repodir
        //         )
        //     )
        // );
        // $json = json_encode($params, JSON_PRETTY_PRINT);
        // $this->log($json);
        // $this->es->snapshot()->createRepository($params);
        // exit;


        // $params = array(
        //     '_id' => 44418,
        //     'woe:id' => 44418,
        //     'iso' => 'GB',
        //     'name' => 'London',
        //     'lang' => 'ENG',
        //     'woe:centroid' => array(-0.12714, 51.506321),
        //     'woe:bbox' => array(
        //         array(-0.563, 51.261318),
        //         array(0.28036, 51.686031)
        //     ),
        //     'woe:bounds' => array(
        //         'type' => 'Polygon',
        //         'coordinates' => array(
        //             array(
        //                 array(-0.563, 51.261),
        //                 array(-0.563, 51.686),
        //                 array(0.28, 51.686),
        //                 array(0.28, 51.261),
        //                 array(-0.563, 51.261)
        //             )
        //         )
        //     ),
        //     'woe:placetype' => 7,
        //     'woe:placetypename' => 'Town',
        //     'woe:parent' => 23416974,
        //     'woe:supercedes' => 1234,
        //     'woe:superceded' => 1234,
        //     "woe:adjacent" => array(
        //         "woe:town=18074",
        //         "woe:town=19919",
        //         "woe:town=19551",
        //         "woe:town=14482",
        //         "woe:town=15786",
        //         "woe:town=11448",
        //         "woe:town=35986",
        //         "woe:town=31287",
        //         "woe:town=30600",
        //         "woe:town=24596",
        //         "woe:town=39684",
        //         "woe:town=26662",
        //         "woe:town=23839",
        //         "woe:town=37214",
        //         "woe:town=39437",
        //         "woe:town=15824",
        //         "woe:town=25681",
        //         "woe:town=21746",
        //         "woe:town=12052",
        //         "woe:town=28278",
        //         "woe:town=18076",
        //         "woe:town=35337",
        //         "woe:town=17535",
        //         "woe:town=17333",
        //         "woe:town=17601",
        //         "woe:town=22218",
        //         "woe:town=21298",
        //         "woe:town=11566",
        //         "woe:town=39690",
        //         "woe:town=29718",
        //         "woe:town=37013",
        //         "woe:town=14729",
        //         "woe:town=39185",
        //         "woe:town=20094177",
        //         "woe:town=56064358",
        //         "woe:town=26712",
        //         "woe:town=23517",
        //         "woe:town=29719",
        //         "woe:town=28976",
        //         "woe:town=15566",
        //         "woe:town=40561",
        //         "woe:town=25556",
        //         "woe:town=22552",
        //         "woe:town=16174",
        //         "woe:town=21490",
        //         "woe:town=28835375",
        //         "woe:town=13267",
        //         "woe:town=39912",
        //         "woe:town=15890",
        //         "woe:town=14384",
        //         "woe:suburb=38838",
        //         "woe:town=14728",
        //         "woe:town=39321",
        //         "woe:town=35295",
        //         "woe:town=27508",
        //         "woe:town=13781",
        //         "woe:town=39070",
        //         "woe:town=35009",
        //         "woe:suburb=17002",
        //         "woe:town=34397",
        //         "woe:town=36880",
        //         "woe:town=39072",
        //         "woe:town=37149",
        //         "woe:town=36749",
        //         "woe:suburb=31092",
        //         "woe:town=11791",
        //         "woe:town=17953",
        //         "woe:town=21536",
        //         "woe:town=27397",
        //         "woe:town=11209",
        //         "woe:town=19055",
        //         "woe:town=33295",
        //         "woe:town=36612",
        //         "woe:town=19643",
        //         "woe:town=39188",
        //         "woe:suburb=33508",
        //         "woe:town=24595",
        //         "woe:town=32682",
        //         "woe:town=19649",
        //         "woe:town=25985",
        //         "woe:town=34560",
        //         "woe:town=39747",
        //         "woe:suburb=38594",
        //         "woe:town=11157",
        //         "woe:town=15405",
        //         "woe:town=19688",
        //         "woe:town=32485",
        //         "woe:town=40385"
        //     ),
        //     "woe:alias_DUT_Q" => array(
        //         "Londen"
        //     ),
        //     'woe:alias_FRE_Q' => array(
        //         "Londres"
        //     ),
        //     'woe:alias_ITA_Q' => array(
        //         "Londra"
        //     ),
        //     'woe:alias_POR_Q' => array(
        //         "Londres"
        //     ),
        //     'woe:alias_SPA_Q' => array(
        //         "Londres"
        //     ),
        //     'woe:alias_JPN_Q' => array(
        //         "ロンドン"
        //     ),
        //     'woe:alias_FIN_Q' => array(
        //         "Lontoo"
        //     ),
        //     'woe:alias_KOR_Q' => array(
        //         "런던"
        //     ),
        //     'woe:alias_CZE_Q' => array(
        //         "Londýn"
        //     ),
        //     'woe:alias_POL_Q' => array(
        //         "Londyn"
        //     ),
        //     'woe:alias_RUM_Q' => array(
        //         "Londra"
        //     ),
        //     'woe:alias_CHI_Q' => array(
        //         "倫敦"
        //     ),
        //     "woe:alias_FIN_V" => array(
        //         "Lontooseen",
        //         "Lontoosta",
        //         "Lontoon kautta"
        //     ),
        //     'woe:alias_ARA_V' => array(
        //         "لندن"
        //     ),
        //     'woe:alias_UNK_V' => array(
        //         "Лондон",
        //         "Λονδινο",
        //         "लंदन",
        //         "लदन",
        //         "Lundúnir",
        //         "Lundunir",
        //         "Londonas",
        //         "ลอนดอน"
        //     ),
        //     'woe:alias_KOR_V' => array(
        //         "런던"
        //     ),
        //     'woe:alias_CHI_V' => array(
        //         "伦敦"
        //     ),
        //     'woe:alias_ENG_V' => array(
        //         "LON"
        //     ),
        //     'history' => array(
        //         array(
        //             'source' => 'GeoPlanet 7.3.1',
        //             'timestamp' => 1403102873
        //         ),
        //         array(
        //             'source' => 'GeoPlanet 7.3.2',
        //             'timestamp' => 1403240483
        //         ),
        //         array(
        //             'source' => 'GeoPlanet 7.4.0',
        //             'timestamp' => 1403417454
        //         ),
        //         array(
        //             'source' => 'GeoPlanet 7.4.1',
        //             'timestamp' => 1403446684
        //         ),
        //         array(
        //             'source' => 'GeoPlanet 7.5.1',
        //             'timestamp' => 1403531938
        //         ),
        //         array(
        //             'source' => 'GeoPlanet 7.5.2',
        //             'timestamp' => 1403588795
        //         ),
        //         array(
        //             'source' => 'GeoPlanet 7.6.0',
        //             'timestamp' => 1403673376
        //         ),
        //         array(
        //             'source' => 'GeoPlanet 7.8.1',
        //             'timestamp' => 1403705517
        //         )
        //     )
        // 'index' => self::DATABASE,
        // 'type' => self::PLACES_TYPE,
        // 'id' => 44418
        // );

        // $json = json_encode($params, JSON_PRETTY_PRINT);
        // $this->log($json);

        // error_log(var_export($this->collections, true));
        // $this->collections[self::PLACES_COLLECTION]->insert($params);
        // $res = $this->get_woeid(44418);
        // error_log(var_export($res, true));
        //
        // $res = $this->get_woeid(44417);
        // error_log(var_export($res, true));

    }

    private function setup_snapshots() {
        $this->logVerbose("Checking for snapshot repository $this->snapshot_repo_name");
        $params = array(
            'repository' => $this->snapshot_repo_name
        );
        try {
            $doc = $this->client->snapshot()->getRepository($params);
            $this->logVerbose("Snapshot repository $this->snapshot_repo_name exists");
        }
        catch (\Exception $e) {
            $this->logVerbose("$this->snapshot_repo_name does not exist, setting up");
            $params['body'] = array(
                'type' =>'fs',
                'settings' => array(
                    'compress' => true,
                    'location' => $this->snapshot_repo_dir
                )
            );
            $this->client->snapshot()->createRepository($params);
            $this->logVerbose("Created snapshot repository $this->snapshot_repo_name");
        }
    }

    private function create_snapshot() {
        $name = $this->history[0]['source'] . '-' . $this->stage;
        $name = strtolower(str_replace(' ', '-', $name));
        $this->log("Creating snapshot $name");
        $params = array(
            'repository' => $this->snapshot_repo_name,
            'snapshot' => $name,
            'wait_for_completion' => true
        );
        $this->client->snapshot()->create($params);
        $this->logVerbose("Finished creating snapshot $name");
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
            throw new Exception("$file:$row - Missing fields $fields");
        }
    }

    private function init_cache(&$db) {
        $db->exec('PRAGMA synchronous=0');
        $db->exec('PRAGMA locking_mode=EXCLUSIVE');
        $db->exec('PRAGMA journal_mode=DELETE');
    }

    private function sanitize_coord($coord) {
        if ($coord == '\N' || $coord == '\n' || $coord == NULL) {
            $coord = 0;
        }

        return $coord;
    }

    private function make_placeholder_woeid($woeid, $placeholder=NULL) {
        $placetype = $this->placetypes->get_by_id(0);
        $doc = array(
            // 'body' => array(
            // '_id' => (int)$woeid,
            // 'woe:id' => (int)$woeid,
            'iso' => '',
            'name' => '',
            'lang' => 'ENG',
            'woe:placetype' => (int)$placetype['placetype']['id'],
            'woe:placetypename' => $placetype['placetype']['name'],
            'woe:parent' => 0,
            'history' => $this->history
            // ),
            // 'index' => self::DATABASE,
            // 'type' => self::PLACES_TYPE,
            // 'id' => (int)$woeid,
            // 'refresh' => true
        );

        if (NULL !== $placeholder) {
            $doc = array_merge($doc, $placeholder);
        }

        $doc['_id'] = (int)$woeid;
        $doc['woe:id'] = (int)$woeid;

        return $doc;
    }

    private function collect_admins($woeid, &$admins) {
        //error_log("Collecting admins for $woeid");
        $doc = $this->get_woeid($woeid);
        if (!$doc['found']) {
            //throw new Exception("Couldn't find record for woeid $woeid");
            //error_log("Skipping admins for non-existant WOEID $woeid");
            return $admins;
        }

        if ($doc['_source']['woe:parent'] == 0 || $doc['_source']['woe:placetype'] == 0) {
            // error_log("Parent or placetype are empty/placeholder");
            return $admins;
        }

        // error_log("Parent: " . $doc['_source']['woe:parent']);
        // error_log("Placetype: " . $doc['_source']['woe:placetype']);
        switch($doc['_source']['woe:placetype']) {
            case 8:    // state
            $admins['woe:state'] = $woeid;
            break;
            case 9:    // county
            $admins['woe:county'] = $woeid;
            break;
            case 10:    // local-admin
            $admins['woe:local-admin'] = $woeid;
            break;
            case 12:    // country
            $admins['woe:country'] = $woeid;
            break;
            case 29:    // continent
            $admins['woe:continent'] = $woeid;
            break;

            default:
            break;
        }

        // error_log("Admins so far:" . var_export($admins, true));
        if ((isset($admins['woe:state']) && !empty($admins['woe:state'])) &&
        (isset($admins['woe:county']) && !empty($admins['woe:county'])) &&
        (isset($admins['woe:local-admin']) && !empty($admins['woe:local-admin'])) &&
        (isset($admins['woe:country']) && !empty($admins['woe:country'])) &&
        (isset($admins['woe:continent']) && !empty($admins['woe:continent']))) {
            // error_log("Admins completed");
            return $admins;
        }

        if ($doc['_source']['woe:parent'] == 1) {
            // error_log("Parent is Earth, no more to collect");
            return $admins;
        }

        // error_log("Recursing into parent " . $doc['_source']['woe:parent']);
        return $this->collect_admins($doc['_source']['woe:parent'], $admins);
    }

    private function make_snapshot() {
        $path = $this->snapshot . '/' . $this->version . '/' . $this->stage;
        if (!is_dir($path)) {
            $mode = 0777;
            $recursive = true;
            mkdir($path, $mode, $recursive);
        }
        $cmd = '/usr/local/bin/mongodump --quiet --out ' . $path . ' --db ' . self::DATABASE;
        $this->log('Creating snapshot: ' . $cmd);
        exec($cmd);
    }
}

$shortopts = "vcm:p:s:S:b:";
$longopts = array(
    "verbose",
    "clear",
    "mongodb:",
    "path:",
    "stage:",
    "snapshot",
    "batch"
);

$verbose = false;
$purge = false;
$path = NULL;
// $instance = 'mongodb://localhost:27017';
$instance = '127.0.0.1:9200';
$stage = NULL;
$snapshot = '~/geoplanet-snapshots';
$batch = 100000;

$options = getopt($shortopts, $longopts);

if (isset($options['v']) || isset($options['verbose'])) {
    $verbose = true;
}
if (isset($options['c']) || isset($options['clear'])) {
    $purge = true;
}
if (isset($options['m'])) {
    $instance = $options['m'];
}
else if (isset($options['mongodb'])) {
    $instance = $options['mongodb'];
}

if (isset($options['p'])) {
    $path = $options['p'];
}
else if (isset($options['path'])) {
    $path = $options['path'];
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

if (isset($options['S'])) {
    $snapshot = $options['S'];
}
else if (isset($options['snapshot'])) {
    $snapshot = $options['snapshot'];
}

if (isset($options['b'])) {
    $batch = $options['b'];
}
else if (isset($options['batch'])) {
    $batch = $options['batch'];
}

$import = new GeoPlanetImport($instance, $path, $verbose, $purge, $stage, $snapshot, $batch);
$import->run();

?>
