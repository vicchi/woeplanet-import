#!/usr/bin/env php
<?php

require_once dirname(__FILE__) . '/../vendor/autoload.php';
// require_once dirname(__FILE__) . '/../lib/timer.php';
// require_once dirname(__FILE__) . '/../lib/placetypes.php';
// require_once dirname(__FILE__) . '/../lib/reader.php';
// require_once dirname(__FILE__) . '/../lib/geometry.php';
// require_once dirname(__FILE__) . '/../lib/task-runner.php';
// require_once dirname(__FILE__) . '/../lib/cache-utils.php';

class CacheImport extends \Woeplanet\Utils\TaskRunner {
    const RUN_TASK = 'run';
    const SETUP_TASK = 'setup';
    const PLACES_TASK = 'places';
    const COORDS_TASK = 'coords';
    const CHANGES_TASK = 'changes';
    const ADJACENCIES_TASK = 'adjacencies';
    const ALIASES_TASK = 'aliases';
    const ADMINS_TASK = 'admins';
    const PLACETYPES_TASK = 'placetypes';
    const COUNTRIES_TASK = 'countries';
    const CHILDREN_TASK = 'children';
    const ANCESTORS_TASK = 'ancestors';
    const SIBLINGS_TASK = 'siblings';
    const DESCENDANTS_TASK = 'descendants';
    const BELONGSTOS_TASK = 'belongstos';
    const TEST_TASK = 'test';

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

    const COUNTRIES_WOEID = 'WOE_ID';
    const COUNTRIES_NAME = 'Name';
    const COUNTRIES_ISO2 = 'ISO2';
    const COUNTRIES_ISO3 = 'ISO3';

    private $path = NULL;
    private $task = NULL;
    private $source;
    private $version;
    private $timestamp;
    private $history = [];

    protected $tasks;
    private $timer;

    private $places;
    private $aliases;
    private $adjacencies;
    private $changes;
    private $coords;
    private $admins;
    private $countries;

    private $files;
    private $placetypes;

    private $cache;

    public function __construct($config) {
        parent::__construct($config['verbose']);

        $this->cache = new \Woeplanet\Utils\GeoplanetCache($config['output']);
        $this->placetypes = new \Woeplanet\Types\PlaceTypes();

        $this->path = $config['input'];

        $this->task = $config['task'];

        $this->tasks = [
            self::RUN_TASK,
            self::SETUP_TASK,
            self::PLACES_TASK,
            self::COORDS_TASK,
            self::CHANGES_TASK,
            self::ADJACENCIES_TASK,
            self::ALIASES_TASK,
            self::ADMINS_TASK,
            self::PLACETYPES_TASK,
            self::COUNTRIES_TASK,
            self::CHILDREN_TASK,
            self::ANCESTORS_TASK,
            self::SIBLINGS_TASK,
            self::DESCENDANTS_TASK,
            self::BELONGSTOS_TASK,
            self::TEST_TASK
        ];

        $match = [];
        $pattern = '/(.+)_([\d\.]+)$/';
        $ret = preg_match($pattern, basename($this->path), $match);
        if ($ret === 1) {
            $this->source = 'GeoPlanet';
            $this->version = $match[2];
            $this->timestamp = time();
            $this->history[] = [
                'source' => sprintf('%s %s', $this->source, $this->version),
                'timestamp' => (int) $this->timestamp
            ];
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
        $this->countries = sprintf('geoplanet_countries_%s.tsv', $this->version);

        $this->files = [];

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

        $this->timer = new \Woeplanet\Utils\Timer($this->tasks);
    }

    public function run() {
        $this->timer->elapsed(self::RUN_TASK);

        if (isset($this->task)) {
            $this->timer->elapsed($this->task);

            $func = "task_$this->task";
            $this->$func();

            $elapsed = $this->timer->seconds_to_time($this->timer->elapsed($this->task));
            $this->log("Completed task $this->task in $elapsed");
        }

        else {
            foreach ($this->tasks as $task) {
                if ($task !== self::RUN_TASK && $task !== self::TEST_TASK) {
                    $this->timer->elapsed($task);

                    $func = "task_$task";
                    $this->task = $task;
                    $this->$func();

                    $elapsed = $this->timer->seconds_to_time($this->timer->elapsed($task));
                    $this->log("Completed task $task in $elapsed");
                }
            }
        }

        $elapsed = $this->timer->seconds_to_time($this->timer->elapsed(self::RUN_TASK));
        $this->log("Completed in $elapsed");
    }

    private function task_setup() {
        $this->cache->create_table(\Woeplanet\Utils\GeoplanetCache::META_TABLE);
        $this->cache->create_table(\Woeplanet\Utils\GeoplanetCache::PLACES_TABLE);
        $this->cache->create_table(\Woeplanet\Utils\GeoplanetCache::ADJACENCIES_TABLE);
        $this->cache->create_table(\Woeplanet\Utils\GeoplanetCache::ALIASES_TABLE);
        $this->cache->create_table(\Woeplanet\Utils\GeoplanetCache::PLACETYPES_TABLE);
        $this->cache->create_table(\Woeplanet\Utils\GeoplanetCache::ADMINS_TABLE);
    }

    private function task_places() {
        $reader = new \Woeplanet\Utils\GeoplanetReader();
        $reader->open($this->files[$this->places]);
        $total = $reader->size();
        $row = 0;
        $batch = 0;
        $max_woeid = 0;
        $params = [];

        $this->cache->create_index(\Woeplanet\Utils\GeoplanetCache::META_TABLE);
        $this->cache->create_index(\Woeplanet\Utils\GeoplanetCache::PLACES_TABLE);

        $this->log("Caching $total places");

        while (($data = $reader->get()) !== false) {
            $row++;
            $this->check_raw_places($row, $data);
            $old = $this->cache->get_woeid($data['WOE_ID']);
            $placetype = $this->placetypes->get_by_shortname($data['PlaceType']);
            if (!$placetype['found']) {
                throw new Exception('Cannot find match for placetype ' . $data['PlaceType'] . ' for WOEID ' . $data['WOE_ID']);
            }

            $woeid = intval($data['WOE_ID']);
            $ptid = intval($placetype['placetype']['id']);
            $parent = intval($data['Parent_ID']);

            $doc = [
                'woeid' => $woeid,
                'name' => $data['Name'],
                'iso' => $data['ISO'],
                'lang' => $data['Language'],
                'placetype' => $ptid,
                'placetypename' => $data['PlaceType'],
                'parent' => $parent,
                'history' => $this->history
            ];
            $this->cache->insert_place($doc);

            if ($old !== NULL) {
                $doc = [
                    'woeid' => $woeid,
                    'history' => $old['history']
                ];

                $doc['history'][] = $this->history[0];
                if (isset($old['supercedes']) && !empty($old['supercedes'])) {
                    $doc['supercedes'] = $old['supercedes'];
                }
                if (isset($old['superceded']) && !empty($old['superceded'])) {
                    $doc['superceded'] = $old['superceded'];
                }

                $this->cache->update_place($doc);
            }

            $this->show_status($row, $total);

            if ($max_woeid <= $data['WOE_ID']) {
                $max_woeid = $data['WOE_ID'];
            }
        }

        $this->cache->refresh_meta($max_woeid);
        $this->log("\nFinished caching $total places");
    }

    private function task_coords() {
        if (false !== array_key_exists($this->coords, $this->files)) {
            $reader = new \Woeplanet\Utils\GeoplanetReader();
            $reader->open($this->files[$this->coords]);
            $total = $reader->size();
            $row = 0;
            $db = $this->cache->get_cache();

            $this->log("Caching $total coords");

            while (($data = $reader->get()) !== false) {
                $row++;
                $this->check_raw_coords($row, $data);
                $woeid = $data['WOE_ID'];

                $lat = $this->sanitize_coord($data['Lat']);
                $lon = $this->sanitize_coord($data['Lon']);

                $nelat = $this->sanitize_coord($data['NE_Lat']);
                $nelon = $this->sanitize_coord($data['NE_Lon']);

                $swlat = $this->sanitize_coord($data['SW_Lat']);
                $swlon = $this->sanitize_coord($data['SW_Lon']);

                $doc = $this->cache->get_woeid($data['WOE_ID']);
                if (!$doc) {
                    error_log('Trying to add coords for non-existant WOEID, creating placeholder for ' . $woeid);

                    $doc = $this->make_placeholder_woeid($woeid);
                    $this->cache->insert_place($doc);
                    $this->cache->refresh_meta($woeid);
                    // $doc = $this->cache->get_woeid($data['WOE_ID']);
                }

                $commit = false;
                $doc = ['woeid' => $woeid];

                $centroid = new \Woeplanet\Types\Centroid($lon, $lat);
                if (!$centroid->is_empty() && $centroid->is_valid()) {
                    $doc['lon'] = $lon;
                    $doc['lat'] = $lat;
                    $commit = true;
                }

                $bbox = new \Woeplanet\Types\BoundingBox($swlon, $swlat, $nelon, $nelat);
                if (!$bbox->is_empty() && $bbox->is_valid()) {
                    $doc['swlon'] =  $swlon;
                    $doc['swlat'] =  $swlat;
                    $doc['nelon'] = $nelon;
                    $doc['nelat'] = $nelat;
                }

                if ($commit && (count($doc) > 1)) {
                    $this->cache->update_place($doc);
                }

                $this->show_status($row, $total);
            }

            $this->log("\nFinished caching $total coords");
        }

        else {
            $this->log("No coords found for $this->coords; skipping");
        }
    }

    private function task_changes() {
        if (false !== array_key_exists($this->changes, $this->files)) {
            $reader = new \Woeplanet\Utils\GeoplanetReader();
            $reader->open($this->files[$this->changes]);
            $total = $reader->size();
            $db = $this->cache->get_cache();

            $row = 0;

            $this->log("Caching $total changes");

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

                $old = $this->cache->get_woeid($old_woeid);
                $new = $this->cache->get_woeid($new_woeid);

                if (!$new) {
                    error_log('Missing definition for new WOEID ' . $new_woeid);
                    // error_log($new_woeid . ' supercedes old ' . $old_woeid);
                    $doc = $this->make_placeholder_woeid($new_woeid);
                    $doc['supercedes'] = [$old_woeid];

                    $this->cache->insert_place($doc);

                    $this->cache->refresh_meta($new_woeid);
                    // $new = $this->cache->get_woeid($new_woeid);
                }
                else {
                    // error_log($new_woeid . ' supercedes old ' . $old_woeid);
                    $supercedes = [];
                    if (isset($new['supercedes']) && !empty($new['supercedes'])) {
                        // error_log('supercedes: ' . var_export($new['supercedes'], true));
                        $supercedes = $new['supercedes'];
                    }
                    // error_log('searching ' . var_export($supercedes, true) . ' for ' . $old_woeid);
                    if (array_search($old_woeid, $supercedes) == false) {
                        $supercedes[] = $old_woeid;
                        $new['supercedes'] = $supercedes;
                        // error_log($new_woeid . ' supercedes old ' . var_export($new['supercedes'], true));

                        $doc = [
                            'woeid' => $new['woeid'],
                            'supercedes' => $new['supercedes']
                        ];
                        $this->cache->update_place($doc);
                    }
                }

                if (!$old) {
                    error_log('Missing definition for old WOEID ' . $old_woeid);
                    // error_log($old_woeid . ' superceded by new ' . $new_woeid);

                    $old = $this->make_placeholder_woeid($old_woeid);
                    $old['superceded'] = $new_woeid;

                    $this->cache->insert_place($old);

                    $this->cache->refresh_meta($old_woeid);
                    // $old = $this->cache->get_woeid($old_woeid);
                }

                else {
                    // error_log($old_woeid . ' superceded by new ' . $new_woeid);

                    $old['superceded'] = $new_woeid;

                    $doc = [
                        'woeid' => $old['woeid'],
                        'superceded' => $old['superceded']
                    ];

                    $this->cache->update_place($doc);
                }

                $this->show_status($row, $total);
            }

            // $this->create_snapshot();
            $this->log("\nFinished caching $total changes");
        }
        else {
            $this->log("No changes found for $this->changes; skipping");
        }
    }

    private function task_adjacencies() {
        $db = $this->cache->get_cache();
        $reader = new \Woeplanet\Utils\GeoplanetReader();
        $reader->open($this->files[$this->adjacencies]);
        $total = $reader->size();

        $this->log('Resetting adjacencies cache');
        $this->cache->create_table(\Woeplanet\Utils\GeoplanetCache::ADJACENCIES_TABLE, true);
        $this->cache->create_index(\Woeplanet\Utils\GeoplanetCache::ADJACENCIES_TABLE);

        $this->log("Pre-caching $total adjacencies");

        $row = 0;
        $sql = "INSERT INTO adjacencies(woeid, neighbour) VALUES(:woeid,:neighbour);";
        $insert = $db->prepare($sql);

        while (($data = $reader->get()) !== false) {
            $row++;
            $this->check_raw_adjacencies($row, $data);

            $insert->bindValue(':woeid', $data['Place_WOE_ID']);
            $insert->bindValue(':neighbour', $data['Neighbour_WOE_ID']);
            $insert->execute();
            $insert->closeCursor();

            $this->show_status($row, $total);
        }

        $this->logVerbose("\nPre-cached $row of $total adjacencies");

        $ids = [];
        $select = "SELECT COUNT(DISTINCT woeid) FROM adjacencies;";
        $statement = $db->prepare($select);
        $statement->execute();
        $result = $statement->fetch(\PDO::FETCH_ASSOC);
        $count = $result['COUNT(DISTINCT woeid)'];
        $statement->closeCursor();

        $this->log("Aggregating and caching $count adjacencies");

        $select = "SELECT DISTINCT(woeid) FROM adjacencies";
        $distinct = $db->prepare($select);
        $distinct->execute();

        $select = "SELECT * FROM adjacencies WHERE woeid = :woeid";
        $statement = $db->prepare($select);
        $row = 0;

        while (($adj = $distinct->fetch(\PDO::FETCH_ASSOC)) !== false) {
            $row++;
            $woeid = $adj['woeid'];
            $statement->bindValue(':woeid', $woeid);
            $statement->execute();
            $adjacent = [];

            while (($neighbour = $statement->fetch(\PDO::FETCH_ASSOC)) !== false) {
                $doc = $this->cache->get_woeid($neighbour['neighbour']);
                if (NULL === $doc) {
                    error_log("WTF ... no record found for neighbour WOEID " . $neighbour['neighbour'] . ", creating empty placeholder");
                    $doc = $this->make_placeholder_woeid($neighbour['neighbour']);
                    $this->cache->insert_place($doc);
                    $this->cache->refresh_meta($neighbour['neighbour']);
                    // $doc = $this->cache->get_woeid($neighbour['neighbour']);
                }

                $placecode = $doc['placetype'];
                $placeentry = $this->placetypes->get_by_id($placecode);
                if (!$placeentry['found']) {
                    // error_log(var_export($doc, true));
                    throw new Exception('Cannot find match for placetype id ' . $placecode . ' for WOEID ' . $neighbour['neighbour']);
                }

                $placetag = $placeentry['placetype']['tag'];
                $adjacent[] = sprintf('%s=%s', $placetag, $neighbour['neighbour']);
            }
            $statement->closeCursor();

            $doc = $this->cache->get_woeid($woeid);
            if (!$doc) {
                error_log("WTF ... no record found for adjacent source WOEID " . $adj['woeid'] . ", creating empty placeholder");
                $doc = $this->make_placeholder_woeid($adj['woeid']);
                $this->cache->insert_place($doc);

                $this->cache->refresh_meta($adj['woeid']);
                // $doc = $this->cache->get_woeid($adj['woeid']);
            }

            $doc = [
                'woeid' => $woeid,
                'adjacent' => $adjacent
            ];
            $this->cache->update_place($doc);
            $this->show_status($row, $count);
        }
        $distinct->closeCursor();

        // $this->create_snapshot();
        $this->log("\nFinished indexing $row of $count adjacencies");
    }

    private function task_aliases() {
        $db = $this->cache->get_cache();
        $reader = new \Woeplanet\Utils\GeoplanetReader();
        $reader->open($this->files[$this->aliases]);
        $total = $reader->size();

        $this->log('Resetting aliases cache');
        $this->cache->create_table(\Woeplanet\Utils\GeoplanetCache::ALIASES_TABLE, true);

        $this->log("Pre-caching and aggregating $total aliases");

        $row = 0;
        $insert = "INSERT INTO aliases(woeid, name, type, lang) VALUES(:woeid,:name,:type,:lang);";
        $statement = $db->prepare($insert);

        while (($data = $reader->get()) !== false) {
            $row++;
            $this->check_raw_aliases($row, $data);

            $woeid = intval($data['WOE_ID']);
            $statement->bindValue(':woeid', $woeid);
            $statement->bindValue(':name', $data['Name']);
            $statement->bindValue(':type', $data['Name_Type']);
            $statement->bindValue(':lang', $data['Language']);

            $statement->execute();
            $statement->closeCursor();
            $this->show_status($row, $total);
        }

        $this->logVerbose("\nCached $row of $total aliases");
        $this->cache->create_index(\Woeplanet\Utils\GeoplanetCache::ALIASES_TABLE);

        $select = "SELECT COUNT(DISTINCT woeid) FROM aliases;";
        $statement = $db->prepare($select);
        $statement->execute();
        $result = $statement->fetch(\PDO::FETCH_ASSOC);
        $count = $result['COUNT(DISTINCT woeid)'];
        $statement->closeCursor();

        $select = "SELECT DISTINCT(woeid) FROM aliases;";
        $distinct = $db->prepare($select);
        $distinct->execute();

        $row = 0;
        $ids = [];
        $this->log("Aggregating $count candidate WOEIDs");
        while (($woeid = $distinct->fetch(\PDO::FETCH_ASSOC)) !== false) {
            $row++;
            $ids[] = $woeid['woeid'];
            $this->show_status($row, $count);
        }
        $distinct->closeCursor();
        $total = count($ids);
        $this->log("Found $total candidate WOEIDs; caching ...");

        $select = "SELECT * FROM aliases WHERE woeid = :woeid";
        $statement = $db->prepare($select);
        $row = 0;

        foreach ($ids as $id) {
            $row++;
            $statement->bindValue(':woeid', $id);
            $statement->execute();

            $aliases = [];
            $q = [];
            $v = [];
            $a = [];
            $s = [];
            $p = [];

            while (($alias = $statement->fetch(\PDO::FETCH_ASSOC)) !== false) {
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

                $element = [
                    'lang' => $lang,
                    'name' => $name
                ];
                switch ($type) {
                    case 'Q': $q[] = $element; break;
                    case 'V': $v[] = $element; break;
                    case 'A': $a[] = $element; break;
                    case 'S': $s[] = $element; break;
                    case 'P': $p[] = $element; break;
                }
            }
            $statement->closeCursor();
            $doc = [
                'woeid' => intval($id)
            ];

            if (!empty($q)) {
                $doc['alias_q'] = $q;
            }
            if (!empty($v)) {
                $doc['alias_v'] = $v;
            }
            if (!empty($a)) {
                $doc['alias_a'] = $a;
            }
            if (!empty($s)) {
                $doc['alias_s'] = $s;
            }
            if (!empty($p)) {
                $doc['alias_p'] = $p;
            }

            $this->cache->update_place($doc);

            $this->show_status($row, $total);
        }

        $this->log("\nFinished caching $total aliases");
        // $this->create_snapshot();
    }

    private function task_admins() {
        if (false !== array_key_exists($this->admins, $this->files)) {
            $db = $this->cache->get_cache();
            $reader = new \Woeplanet\Utils\GeoplanetReader();
            $reader->open($this->files[$this->admins]);
            $total = $reader->size();

            // $this->log('Resetting admins cache');
            // $this->cache->create_table(\Woeplanet\Utils\GeoplanetCache::ADMINS_TABLE, true);
            $this->log("Caching $total admins");
            $row = 0;

            // Prior to v10.0, the admins file contained the English version of the admin name, not the woeid
            // If we've got one of those versions, skip updating admins.

            $valid_admins = 0;
            $invalid_admins = 0;

            while (($data = $reader->get()) !== false) {
                $numeric_admin = false;
                $row++;
                $this->check_raw_admins($row, $data);

                $doc = [
                    'woeid' => intval($data['WOE_ID'])
                ];

                if (!empty($data['State']) && is_numeric($data['State'])) {
                    $doc['state'] = intval($data['State']);
                    $numeric_admin = true;
                }
                if (!empty($data['County']) && is_numeric($data['County'])) {
                    $doc['county'] = intval($data['County']);
                    $numeric_admin = true;
                }
                if (!empty($data['Local_Admin']) && is_numeric($data['Local_Admin'])) {
                    $doc['localadmin'] = intval($data['Local_Admin']);
                    $numeric_admin = true;
                }
                if (!empty($data['Country']) && is_numeric($data['Country'])) {
                    $doc['country'] = intval($data['Country']);
                    $numeric_admin = true;
                }
                if (!empty($data['Continent']) && is_numeric($data['Continent'])) {
                    $doc['continent'] = intval($data['Continent']);
                    $numeric_admin = true;
                }

                if ($numeric_admin && (count($doc) > 1)) {
                    $valid_admins++;
                    // $this->cache->insert_admin($doc);
                    $this->cache->update_place($doc);
                }
                else {
                    $invalid_admins++;
                }

                $this->show_status($row, $total);
            }

            // $this->cache->create_index(\Woeplanet\Utils\GeoplanetCache::ADMINS_TABLE);
            $this->log("\nFinished caching $total admins (valid: $valid_admins, invalid: $invalid_admins)");
        }

        else {
            $this->log("No admins found for $this->admins; skipping");
        }
    }

    private function task_placetypes() {
        $db = $this->cache->get_cache();
        $placetypes = $this->placetypes->get();
        $total = count($placetypes);
        $row = 0;
        $this->log("Caching $total placetypes");

        $sql = 'INSERT OR REPLACE INTO placetypes(id, name, descr, shortname, tag) VALUES(:id,:name,:descr,:shortname,:tag);';
        $insert = $db->prepare($sql);
        if (!$insert) {
            error_log('task_placetypes: ' . var_export($db->errorInfo(), true));
        }

        foreach ($placetypes as $placetype) {
            $row++;

            $values = [
                ':id' => intval($placetype['id']),
                ':name' => $placetype['name'],
                ':descr' => $placetype['description'],
                ':shortname' => $placetype['shortname'],
                ':tag' => $placetype['tag']
            ];
            if (!$insert->execute($values)) {
                error_log('task_placetypes: ' . var_export($db->errorInfo(), true));
            }
            $this->show_status($row, $total);
        }
        $insert->closeCursor();

        $this->log("\nFinished caching $total placetypes");
        $this->cache->create_index(\Woeplanet\Utils\GeoplanetCache::PLACETYPES_TABLE);
    }

    private function task_countries() {
        if (false !== array_key_exists($this->countries, $this->files)) {
            $reader = new \Woeplanet\Utils\GeoplanetReader();
            $reader->open($this->files[$this->countries]);
            $total = $reader->size();

            $this->cache->create_table(\Woeplanet\Utils\GeoplanetCache::COUNTRIES_TABLE);

            $this->log("Caching $total countries");

            while (($data = $reader->get()) !== false) {
                $row++;
                $this->check_raw_countries($row, $data);

                $doc = [
                    'woeid' => intval($data['WOE_ID']),
                    'name' => $data['Name'],
                    'iso2' => $data['ISO2'],
                    'iso3' => $data['ISO3']
                ];
                $this->cache->insert_country($doc);

                $this->show_status($row, $total);
            }

            $this->cache->create_index(\Woeplanet\Utils\GeoplanetCache::COUNTRIES_TABLE);
            $this->log("\nFinished caching $total countries");
        }
        else {
            $this->log("No admins found for $this->admins; skipping");
        }
    }

    private function task_children() {
        $db = $this->cache->get_cache();

        $this->log('Resetting children cache');
        $this->cache->create_table(\Woeplanet\Utils\GeoplanetCache::CHILDREN_TABLE, true);

        $this->log('Preparing queries');
        $sql = 'SELECT woeid,placetype FROM places WHERE parent = :parent;';
        $select = $db->prepare($sql);
        if (!$select) {
            error_log('cache_children/prepare select: ' . var_export($db->errorInfo(), true));
        }

        $sql = 'INSERT INTO children(woeid, children) VALUES(:woeid,:children);';
        $insert = $db->prepare($sql);
        if (!$insert) {
            error_log('cache_children/prepare insert: ' . var_export($db->errorInfo(), true));
        }

        $maxwoeid = $this->cache->get_maxwoeid();
        $this->log("Aggregating and caching children for $maxwoeid candidate woeids");

        $sql = 'SELECT woeid FROM places;';
        $find = $db->prepare($sql);
        $find->execute();

        while (($doc = $find->fetch(\PDO::FETCH_ASSOC)) !== false) {
            $woeid = intval($doc['woeid']);
            if ($woeid === 1) {
                continue;
            }

            $children = [];
            $select->bindParam(':parent', $woeid);
            if (!$select->execute()) {
                error_log('cache_children/select: ' . var_export($db->errorInfo(), true));
            }

            else {
                while (($doc = $select->fetch(\PDO::FETCH_ASSOC)) !== false) {
                    $children[] = [
                        intval($doc['placetype']),
                        intval($doc['woeid'])
                    ];
                }
                $values = [
                    ':woeid' => intval($woeid),
                    ':children' => serialize($children)
                ];
                if (!$insert->execute($values)) {
                    error_log('cache_children/insert: ' . var_export($db->errorInfo(), true));
                }
            }

            $this->show_status($woeid, $maxwoeid);
        }

        $this->cache->create_index(\Woeplanet\Utils\GeoplanetCache::CHILDREN_TABLE);
        $this->log("Finished aggregating and caching children for $maxwoeid candidate woeids");
    }

    private function task_ancestors() {
        $db = $this->cache->get_cache();

        $this->log('Resetting ancestors cache');
        $this->cache->create_table(\Woeplanet\Utils\GeoplanetCache::ANCESTORS_TABLE, true);

        // $db->exec('DROP TABLE IF EXISTS ancestors');
        // $this->define_table($this->stage);

        $maxwoeid = $this->cache->get_maxwoeid();

        $sql = 'INSERT INTO ancestors(woeid, ancestors) VALUES(:woeid,:ancestors);';
        $insert = $db->prepare($sql);

        $sql = 'SELECT woeid FROM places;';
        $select = $db->prepare($sql);
        $select->execute();

        $this->log("Aggregating and caching ancestors for $maxwoeid candidate woeids");

        while (($doc = $select->fetch(\PDO::FETCH_ASSOC)) !== false) {
            $woeid = $target = $doc['woeid'];
            $parents = [];

            while ($p = $this->cache->get_parent($target)) {
                if (in_array($p, $parents)) {
                    throw new Exception("Recursion trap: $p is an ancestor of itself!");
                }
                $parents[] = $p;
                $target = $p;
            }

            if (count($parents) == 0) {
                continue;
            }

            $insert->bindValue(':woeid', $woeid);
            $insert->bindValue(':ancestors', serialize($parents));

            if (!$insert->execute()) {
                error_log('cache_ancestors/insert: ' . var_export($db->errorInfo(), true));
            }

            $this->show_status($woeid, $maxwoeid);
        }

        $this->cache->create_index(\Woeplanet\Utils\GeoplanetCache::ANCESTORS_TABLE);
        $this->log("Finished aggregating and caching ancestors for $maxwoeid candidate woeids");
    }

    private function task_siblings() {
        $db = $this->cache->get_cache();

        $this->log('Resetting siblings cache');
        $this->cache->create_table(\Woeplanet\Utils\GeoplanetCache::SIBLINGS_TABLE, true);

        $maxwoeid = $this->cache->get_maxwoeid();

        $sql = 'INSERT INTO siblings(woeid, siblings) VALUES(:woeid,:siblings);';
        $insert = $db->prepare($sql);

        $sql = 'SELECT woeid,parent,placetype FROM places;';
        $select = $db->prepare($sql);
        $select->execute();

        $row = 0;
        $this->log("Calculating and caching siblings for $maxwoeid candidate woeids");
        while (($doc = $select->fetch(\PDO::FETCH_ASSOC)) !== false) {
            $row++;
            $woeid = $doc['woeid'];
            $parent = $doc['parent'];
            if (!isset($doc['parent']) || empty($doc['parent'])) {
                continue;   // No parent? Unlikely
            }

            // error_log('woeid: '  .$woeid);
            // error_log('parent: ' . $parent);
            // error_log('get children for parent ' . $parent);
            $children = $this->cache->get_children($parent);
            // error_log(var_export($children, true));
            if ($children === NULL || empty($children)) {
                continue;
            }
            // PDO can't handle binding array values to query parameters, so for
            //
            // "SELECT woeid FROM places WHERE woeid IN (:children) AND placetype = :placetype"
            //
            // PDO should give me this: SELECT woeid FROM places WHERE woeid IN (1,2,3,4,5) AND placetype = 8
            // But it actually gives me this: SELECT woeid FROM places WHERE woeid IN ('1,2,3,4,5') AND placetype = 8
            // Which is pants.

            $children_woeids = [];
            foreach ($children as $child) {
                $children_woeids[] = $child[1];
            }

            $child_woeids = implode(',', $children_woeids);
            $placetype = intval($doc['placetype']);

            $sql = 'SELECT woeid FROM places WHERE woeid IN (' . $child_woeids . ') AND placetype = ' . $placetype . ';';
            $search = $db->prepare($sql);
            if (!$search) {
                error_log('sql: ' . $sql);
                error_log('cache_siblings/search: ' . var_export($db->errorInfo(), true));
                continue;
            }

            if (!$search->execute()) {
                error_log('cache_siblings/search: ' . var_export($db->errorInfo(), true));
                continue;
            }

            $siblings = [];
            while(($sibling = $search->fetch(PDO::FETCH_ASSOC)) !== FALSE) {
                $siblings[] = intval($sibling['woeid']);
            }

            $values = [
                ':woeid' => $doc['woeid'],
                ':siblings' => serialize($siblings)
            ];
            if (!$insert->execute($values)) {
                error_log('cache_siblings/insert: ' . var_export($db->errorInfo(), true));
                continue;
            }

            $this->show_status($row, $maxwoeid);
        }

        $this->cache->create_index(\Woeplanet\Utils\GeoplanetCache::SIBLINGS_TABLE);
        $this->log("Finished calculating and caching siblings for $maxwoeid candidate woeids");
    }

    private function task_descendants() {
        $db = $this->cache->get_cache();

        $this->log('Resetting descendants cache');
        $this->cache->create_table(\Woeplanet\Utils\GeoplanetCache::DESCENDANTS_TABLE, true);

        $maxwoeid = $this->cache->get_maxwoeid();

        $sql = 'SELECT woeid FROM places;';
        $select = $db->prepare($sql);

        $this->log("Calculating and caching descendants for $maxwoeid candidate woeids");
        $row = 0;

        $select->execute();
        // error_log("Recursing into find_descendants");
        while (($doc = $select->fetch(PDO::FETCH_ASSOC)) !== false) {
            $row++;
            $woeid = intval($doc['woeid']);
            if ($woeid === 1) {
                continue;
            }
            $this->find_descendants(intval($doc['woeid']));
            $this->show_status($row, $maxwoeid);
        }

        $this->cache->create_index(\Woeplanet\Utils\GeoplanetCache::DESCENDANTS_TABLE);
        $this->log("Finished calculating and caching descendants for $maxwoeid candidate woeids");
    }

    private function task_belongstos() {
    }


    private function task_test() {
        $woeid = 44418;
        $doc = $this->cache->get_woeid($woeid);
        error_log(var_export($doc, true));

        // $woeid = 44418;
        // $doc = $this->cache->get_woeid($woeid);
        // error_log(var_export($doc, true));

    }

    private function check_raw_places($row, $data) {
        $fields = [
            self::PLACES_WOEID,
            self::PLACES_ISO,
            self::PLACES_NAME,
            self::PLACES_LANGUAGE,
            self::PLACES_PLACETYPE,
            self::PLACES_PARENTID
        ];

        $this->check_raw_data($this->places, $row, $data, $fields);
    }

    private function check_raw_coords($row, $data) {
        $fields = [
            self::COORDS_WOEID,
            self::COORDS_LAT,
            self::COORDS_LON,
            self::COORDS_NELAT,
            self::COORDS_NELON,
            self::COORDS_SWLAT,
            self::COORDS_SWLON
        ];

        $this->check_raw_data($this->coords, $row, $data, $fields);
    }

    private function check_raw_changes($row, $data) {
        $fields = [
            self::CHANGES_WOEID,
            self::CHANGES_REPID
        ];

        $this->check_raw_data($this->changes, $row, $data, $fields);
    }

    private function check_raw_adjacencies($row, $data) {
        $fields = [
            self::ADJACENCIES_WOEID,
            self::ADJACENCIES_NEIGHBOUR
        ];

        $this->check_raw_data($this->adjacencies, $row, $data, $fields);
    }

    private function check_raw_aliases($row, $data) {
        $fields = [
            self::ALIAS_WOEID,
            self::ALIAS_NAME,
            self::ALIAS_TYPE,
            self::ALIAS_LANG
        ];

        $this->check_raw_data($this->aliases, $row, $data, $fields);
    }

    private function check_raw_admins($row, $data) {
        $fields = [
            self::ADMINS_WOEID,
            self::ADMINS_ISO,
            self::ADMINS_STATE,
            self::ADMINS_COUNTY,
            self::ADMINS_LOCALADMIN,
            self::ADMINS_COUNTRY,
            self::ADMINS_CONTINENT
        ];

        $this->check_raw_data($this->admins, $row, $data, $fields);
    }

    private function check_raw_countries($row, $data) {
        $fields = [
            self::COUNTRIES_WOEID,
            self::COUNTRIES_NAME,
            self::COUNTRIES_ISO2,
            self::COUNTRIES_ISO3
        ];

        $this->check_raw_data($this->countries, $row, $data, $fields);
    }

    private function check_raw_data($file, $row, $data, $fields) {
        $missing = [];

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

    private function sanitize_coord($coord) {
        if ($coord == '\N' || $coord == '\n' || $coord == NULL) {
            $coord = 0;
        }

        return $coord;
    }

    private function make_placeholder_woeid($woeid, $placeholder=NULL) {
        $placetype = $this->placetypes->get_by_id(0);
        $doc = [
            // 'body' => [
            // '_id' => (int)$woeid,
            // 'woe:id' => (int)$woeid,
            'iso' => '',
            'name' => '',
            'lang' => 'ENG',
            'placetype' => (int)$placetype['placetype']['id'],
            'placetypename' => $placetype['placetype']['name'],
            'parent' => 0,
            'history' => $this->history
            // ],
            // 'index' => self::DATABASE,
            // 'type' => self::PLACES_TYPE,
            // 'id' => (int)$woeid,
            // 'refresh' => true
        ];

        if (NULL !== $placeholder) {
            // error_log('merging in placeholder:');
            // error_log(var_export($placeholder, true));
            $doc = array_merge($doc, $placeholder);
        }

        $doc['woeid'] = (int)$woeid;

        return $doc;
    }
}


$verbose = false;
$path = NULL;
$task = NULL;
$cache = 'geoplanet_cache.sqlite3';

// -v --verbose
// -t --task 'task-name'
// -i --input 'path-to-geoplanet-data'
// -o --output 'path-to-cache'

$shortopts = 'vt:i:o:';
$longopts = ['verbose', 'task:', 'input:', 'output:'];
$config = [
    'verbose' => false,
    'task' => NULL,
    'input' => NULL,
    'output' => $cache
];

$options = getopt($shortopts, $longopts);

if (isset($options['v']) || isset($options['verbose'])) {
    $config['verbose'] = true;
}

if (isset($options['t'])) {
    $config['task'] = $options['t'];
}
else if (isset($options['task'])) {
    $config['task'] = $options['task'];
}

if (isset($options['i'])) {
    $config['input'] = $options['i'];
}
else if (isset($options['input'])) {
    $config['input'] = $options['input'];
}

if (empty($config['input'])) {
    echo "Missing required input path to GeoPlanet Data\n";
    exit;
}

$importer = new CacheImport($config);
$importer->run();

?>
