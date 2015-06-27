#!/usr/bin/env php
<?php

// require_once 'runner.php';
require_once '../lib/timer.php';
require_once '../lib/placetypes.php';
require_once '../lib/reader.php';
require_once '../lib/geometry.php';
require_once '../lib/task-runner.php';
require_once '../lib/cache-utils.php';

class CacheGeoPlanet extends Woeplanet\TaskRunner {
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
    private $history = array();

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

    public function __construct($path, $verbose, $task=NULL) {
        parent::__construct($verbose);

        $this->cache = new \Woeplanet\CacheUtils('geoplanet_cache.sqlite3');
        $this->placetypes = new \Woeplanet\PlaceTypes();

        $this->path = $path;

        if ($task !== NULL) {
            $task = strtolower($task);
        }
        $this->task = $task;

        $this->tasks = array(
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
            self::TEST_TASK
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
        $this->countries = sprintf('geoplanet_countries_%s.tsv', $this->version);

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

        $this->timer = new Woeplanet\Timer($this->tasks);
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
        $this->cache->create_table(\Woeplanet\CacheUtils::META_TABLE);
        $this->cache->create_table(\Woeplanet\CacheUtils::PLACES_TABLE);
        $this->cache->create_table(\Woeplanet\CacheUtils::ADJACENCIES_TABLE);
        $this->cache->create_table(\Woeplanet\CacheUtils::ALIASES_TABLE);
        $this->cache->create_table(\Woeplanet\CacheUtils::PLACETYPES_TABLE);
        $this->cache->create_table(\Woeplanet\CacheUtils::ADMINS_TABLE);
    }

    private function task_places() {
        $reader = new \Woeplanet\Reader();
        $reader->open($this->files[$this->places]);
        $total = $reader->size();
        $row = 0;
        $batch = 0;
        $max_woeid = 0;
        $params = array();

        $this->cache->create_index(\Woeplanet\CacheUtils::META_TABLE);
        $this->cache->create_index(\Woeplanet\CacheUtils::PLACES_TABLE);

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

            $doc = array(
                'woeid' => $woeid,
                'name' => $data['Name'],
                'iso' => $data['ISO'],
                'lang' => $data['Language'],
                'placetype' => $ptid,
                'placetypename' => $data['PlaceType'],
                'parent' => $parent,
                'history' => $this->history
            );
            $this->cache->insert_place($doc);

            if ($old !== NULL) {
                $doc = array(
                    'woeid' => $woeid,
                    'history' => $old['history']
                );

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
            $reader = new \Woeplanet\Reader();
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
                $doc = array('woeid' => $woeid);

                $centroid = new \Woeplanet\Centroid($lon, $lat);
                if (!$centroid->is_empty() && $centroid->is_valid()) {
                    $doc['lon'] = $lon;
                    $doc['lat'] = $lat;
                    $commit = true;
                }

                $bbox = new \Woeplanet\BoundingBox($swlon, $swlat, $nelon, $nelat);
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
            $reader = new \Woeplanet\Reader();
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
                    $doc['supercedes'] = array($old_woeid);

                    $this->cache->insert_place($doc);

                    $this->cache->refresh_meta($new_woeid);
                    // $new = $this->cache->get_woeid($new_woeid);
                }
                else {
                    // error_log($new_woeid . ' supercedes old ' . $old_woeid);
                    $supercedes = array();
                    if (isset($new['supercedes']) && !empty($new['supercedes'])) {
                        // error_log('supercedes: ' . var_export($new['supercedes'], true));
                        $supercedes = $new['supercedes'];
                    }
                    // error_log('searching ' . var_export($supercedes, true) . ' for ' . $old_woeid);
                    if (array_search($old_woeid, $supercedes) == false) {
                        $supercedes[] = $old_woeid;
                        $new['supercedes'] = $supercedes;
                        // error_log($new_woeid . ' supercedes old ' . var_export($new['supercedes'], true));

                        $doc = array('woeid' => $new['woeid'], 'supercedes' => $new['supercedes']);
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

                    $doc = array('woeid' => $old['woeid'], 'superceded' => $old['superceded']);

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
        $reader = new \Woeplanet\Reader();
        $reader->open($this->files[$this->adjacencies]);
        $total = $reader->size();

        $this->log('Resetting adjacencies cache');
        $this->cache->create_table(\Woeplanet\CacheUtils::ADJACENCIES_TABLE, true);
        $this->cache->create_index(\Woeplanet\CacheUtils::ADJACENCIES_TABLE);

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

        $ids = array();
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
            $adjacent = array();

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

            $doc = array('woeid' => $woeid, 'adjacent' => $adjacent);
            $this->cache->update_place($doc);
            $this->show_status($row, $count);
        }
        $distinct->closeCursor();

        // $this->create_snapshot();
        $this->log("\nFinished indexing $row of $count adjacencies");
    }

    private function task_aliases() {
        $db = $this->cache->get_cache();
        $reader = new \Woeplanet\Reader();
        $reader->open($this->files[$this->aliases]);
        $total = $reader->size();

        $this->log('Resetting aliases cache');
        $this->cache->create_table(\Woeplanet\CacheUtils::ALIASES_TABLE, true);

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
        $this->cache->create_index(\Woeplanet\CacheUtils::ALIASES_TABLE);

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
        $ids = array();
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

            $aliases = array();
            $q = array();
            $v = array();
            $a = array();
            $s = array();
            $p = array();

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

                $element = array('lang' => $lang, 'name' => $name);
                switch ($type) {
                    case 'Q': $q[] = $element; break;
                    case 'V': $v[] = $element; break;
                    case 'A': $a[] = $element; break;
                    case 'S': $s[] = $element; break;
                    case 'P': $p[] = $element; break;
                }
            }
            $statement->closeCursor();
            $doc = array('woeid' => intval($id));

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
            $reader = new \Woeplanet\Reader();
            $reader->open($this->files[$this->admins]);
            $total = $reader->size();

            $this->log('Resetting admins cache');
            $this->cache->create_table(\Woeplanet\CacheUtils::ADMINS_TABLE, true);
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

                $doc = array('woeid' => intval($data['WOE_ID']));

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
                    $this->cache->insert_admin($doc);
                }
                else {
                    $invalid_admins++;
                }

                $this->show_status($row, $total);
            }

            $this->cache->create_index(\Woeplanet\CacheUtils::ADMINS_TABLE);
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

            $values = array(
                ':id' => intval($placetype['id']),
                ':name' => $placetype['name'],
                ':descr' => $placetype['description'],
                ':shortname' => $placetype['shortname'],
                ':tag' => $placetype['tag']
            );
            if (!$insert->execute($values)) {
                error_log('task_placetypes: ' . var_export($db->errorInfo(), true));
            }
            $this->show_status($row, $total);
        }
        $insert->closeCursor();

        $this->log("\nFinished caching $total placetypes");
        $this->cache->create_index(\Woeplanet\CacheUtils::PLACETYPES_TABLE);
    }

    private function task_countries() {
        if (false !== array_key_exists($this->admins, $this->files)) {
            $reader = new \Woeplanet\Reader();
            $reader->open($this->files[$this->countries]);
            $total = $reader->size();

            $this->cache->create_table(\Woeplanet\CacheUtils::COUNTRIES_TABLE);

            $this->log("Caching $total countries");

            while (($data = $reader->get()) !== false) {
                $row++;
                $this->check_raw_countries($row, $data);

                $doc = array('woeid' => intval($data['WOE_ID']), 'name' => $data['name'], 'iso2' => $data['ISO2'], 'iso3' => $data['ISO3']);
                $this->cache->insert_country($doc);

                $this->show_status($row, $total);
            }

            $this->cache->create_index(\Woeplanet\CacheUtils::COUNTRIES_TABLE);
            $this->log("\nFinished caching $total countries");
        }
        else {
            $this->log("No admins found for $this->admins; skipping");
        }
    }

    private function task_test() {
        $woeid = 29831;
        $doc = $this->cache->get_woeid($woeid);
        error_log(var_export($doc, true));

        // $woeid = 44418;
        // $doc = $this->cache->get_woeid($woeid);
        // error_log(var_export($doc, true));

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

    private function check_raw_countries($row, $data) {
        $fields = array(self::COUNTRIES_WOEID, self::COUNTRIES_NAME, self::COUNTRIES_ISO2, self::COUNTRIES_ISO3);

        $this->check_raw_data($this->countries, $row, $data, $fields);
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
            'placetype' => (int)$placetype['placetype']['id'],
            'placetypename' => $placetype['placetype']['name'],
            'parent' => 0,
            'history' => $this->history
            // ),
            // 'index' => self::DATABASE,
            // 'type' => self::PLACES_TYPE,
            // 'id' => (int)$woeid,
            // 'refresh' => true
        );

        if (NULL !== $placeholder) {
            // error_log('merging in placeholder:');
            // error_log(var_export($placeholder, true));
            $doc = array_merge($doc, $placeholder);
        }

        $doc['woeid'] = (int)$woeid;

        return $doc;
    }

    private function collect_admins($woeid, &$admins) {
        //error_log("Collecting admins for $woeid");
        $doc = $this->cache->get_woeid($woeid);
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
}


$shortopts = "vp:t:";
$longopts = array(
    "verbose",
    "path:",
    "task:",
);

$verbose = false;
$path = NULL;
$task = NULL;

$options = getopt($shortopts, $longopts);

if (isset($options['v']) || isset($options['verbose'])) {
    $verbose = true;
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

if (isset($options['t'])) {
    $task = $options['t'];
}
else if (isset($options['task'])) {
    $task = $options['task'];
}

$cache = new CacheGeoPlanet($path, $verbose, $task);
$cache->run();

?>
