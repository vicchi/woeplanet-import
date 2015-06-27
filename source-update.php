#!/usr/bin/env php
<?php

require_once 'timer.php';
require_once 'reader.php';
require_once 'task-runner.php';
require_once 'cache-utils.php';

class SourceUpdate extends Woeplanet\TaskRunner {
    const RUN_TASK = 'run';
    const UPDATE_TASK = 'update';
    const GAZETTEER_TASK = 'gazetteer';
    const COORDS_TASK = 'coords';
    const TEST_TASK = 'test';

    private $path = NULL;
    private $qspath = NULL;
    private $task = NULL;
    private $source;
    private $version;
    private $timestamp;
    private $history = array();

    protected $tasks;
    private $timer;

    private $places;
    private $timezones;
    private $coords;
    private $concordance;

    private $files;
    private $placetypes;

    private $cache;

    public function __construct($path, $qspath, $verbose, $task=NULL) {
        parent::__construct($verbose);

        $this->path = $path;
        $this->qspath = $qspath;

        if ($task !== NULL) {
            $task = strtolower($task);
        }
        $this->task = $task;

        $this->tasks = array(
            self::RUN_TASK,
            self::UPDATE_TASK,
            self::GAZETTEER_TASK,
            self::COORDS_TASK
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
        $this->timezones = sprintf('geoplanet_timezones_%s.tsv', $this->version);
        $this->coords = sprintf('geoplanet_coords_%s.tsv', $this->version);
        $this->concordance = sprintf('geoplanet_concordance_%s.tsv', $this->version);

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
        if (false === array_key_exists($this->coords, $this->files)) {
            $this->log("Missing $this->coords");
            exit;
        }

        $this->updates = new \Woeplanet\CacheUtils('geoplanet_updates.sqlite3', false);
        $this->cache = new \Woeplanet\CacheUtils('geoplanet_cache.sqlite3');

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

    private function task_update() {
        $reader = new \Woeplanet\Reader();
        $places_file = $this->path . '/' . 'geoplanet_places_updated_' . $this->version . '.tsv';
        $tz_file = $this->path . '/' . 'geoplanet_timezones_' . $this->version . '.tsv';
        $logfile = $this->path . '/' . 'update.log';

        $places = array();
        $timezones = array();

        if (!$pfile = fopen($places_file, 'w')) {
            error_log("Cannot open updated places file $places_file");
            exit;
        }
        if (!$tfile = fopen($tz_file, 'w')) {
            error_log("Cannot open new timezones file $places_file");
            exit;
        }
        if (!$lfile = fopen ($logfile, 'w')) {
            error_log("Cannot open log file $logfile");
            exit;
        }

        $places_header = array('WOE_ID', 'ISO', 'Name', 'Language', 'PlaceType', 'Parent_ID');
        $tz_header = array('WOE_ID', 'TimeZone_ID', 'TimeZone');

        fwrite($pfile, implode("\t", $places_header) . PHP_EOL);
        fwrite($tfile, implode("\t", $tz_header) . PHP_EOL);

        $reader->open($this->files[$this->places]);
        $total = $reader->size();
        $row = 0;
        $db = $this->updates->get_cache();

        $sql = 'SELECT * FROM geoplanet_update WHERE woeid = :woeid;';
        $find = $db->prepare($sql);
        if (!$find) {
            error_log(var_export($db->errorInfo(), true));
            return;
        }

        $this->log("Updating $total woeids");

        while (($data = $reader->get()) !== false) {
            $row++;
            $woeid = intval($data['WOE_ID']);
            $find->bindValue(':woeid', $woeid, \PDO::PARAM_INT);
            if (!$find->execute()) {
                error_log(var_export($db->errorInfo(), true));
                return;
            }

            if (($update = $find->fetch(\PDO::FETCH_ASSOC)) !== FALSE) {
                $src_parent = intval($data['Parent_ID']);
                $upd_parent = intval($update['parent']);
                $upd_tz = intval($update['timezone']);

                if ($upd_parent === 0 || $upd_parent === NULL || $upd_tz === 0) {
                    // error_log("Empty values for woeid $woeid!");
                    // error_log(var_export($update, true));
                    fwrite($lfile, $woeid . PHP_EOL);
                    // continue;
                }
                else if ($src_parent !== 0 && $src_parent !== $upd_parent) {
                    // error_log("Parent update for $woeid: $src_parent -> $upd_parent");
                    $data['Parent_ID'] = $upd_parent;
                }
            }

            $data['ISO'] = '"' . $data['ISO'] . '"';
            $data['Name'] = '"' . $data['Name'] . '"';
            fwrite($pfile, implode("\t", $data) . PHP_EOL);

            $tz = array(intval($update['woeid']), intval($update['timezone']), $update['tz']);
            fwrite($tfile, implode("\t", $tz) . PHP_EOL);

            $this->show_status($row, $total);
        }

        $this->log("Finished updating $total woeids");
    }

    private function task_gazetteer() {
        $reader = new \Woeplanet\Reader();
        // $greader = new \Woeplanet\Reader();

        $reader->open($this->files[$this->coords]);
        // $gazetteer_file = $this->qspath . '/quattroshapes_gazetteer_gp_then_gn/shp/quattroshapes_gazetteer_gp_then_gn.csv';
        // $greader->open($gazetteer_file);

        $db = $this->cache->get_cache();

        $total = $reader->size();
        $row = 0;

        $this->cache->create_table(\Woeplanet\CacheUtils::COORDS_TABLE, true);

        $sql = 'INSERT INTO coords(woeid,lon,lat,swlon,swlat,nelon,nelat) VALUES(:woeid,:lon,:lat,:swlon,:swlat,:nelon,:nelat);';
        $insert = $db->prepare($sql);

        $this->log("Pre-caching $total coords");

        while (($data = $reader->get()) !== false) {
            $row++;
            $woeid = $data['WOE_ID'];

            $lat = $this->sanitize_coord($data['Lat']);
            $lon = $this->sanitize_coord($data['Lon']);

            $nelat = $this->sanitize_coord($data['NE_Lat']);
            $nelon = $this->sanitize_coord($data['NE_Lon']);

            $swlat = $this->sanitize_coord($data['SW_Lat']);
            $swlon = $this->sanitize_coord($data['SW_Lon']);

            $insert->bindValue(':woeid', $woeid);
            $insert->bindValue(':lon', $lon);
            $insert->bindValue(':lat', $lat);
            $insert->bindValue(':swlon', $swlon);
            $insert->bindValue(':swlat', $swlat);
            $insert->bindValue(':nelon', $nelon);
            $insert->bindValue(':nelat', $nelat);
            $insert->execute();

            $this->show_status($row, $total);
        }

        $this->cache->create_index(\Woeplanet\CacheUtils::COORDS_TABLE, true);

        $this->log("Pre-cached $total coords");

        $reader->close();
        $gazetteer_file = $this->qspath . '/quattroshapes_gazetteer_gp_then_gn/shp/quattroshapes_gazetteer_gp_then_gn.csv';
        $reader->open($gazetteer_file, ',');
        $total = $reader->size();
        $row = 0;

        $header = array('WOE_ID', 'GeoNames_ID', 'QuattroShapes_ID');
        if (!$fp = fopen($this->path . '/'. $this->concordance, 'w')) {
            error_log("Cannot open new concordance file $this->concordance");
            exit;
        }
        fwrite($fp, implode("\t", $header) . PHP_EOL);

        $this->log("Pre-caching $total gazetteer entries");

        $sql = 'SELECT * FROM coords WHERE woeid = :woeid;';
        $select = $db->prepare($sql);
        if (!$select) {
            error_log($sql);
            error_log('prepare failed: ' . var_export($db->errorInfo(), true));
            exit;
        }
        $sql = 'UPDATE coords SET lon=:lon, lat=:lat WHERE woeid = :woeid;';
        $update = $db->prepare($sql);
        if (!$update) {
            error_log($sql);
            error_log('prepare failed: ' . var_export($db->errorInfo(), true));
            exit;
        }

        $swlon = $swlat = $nelon = $nelat = 0.0;

        while (($data = $reader->get()) !== false) {
            $row++;
            $lon = $this->sanitize_coord(floatval($data['X']));
            $lat = $this->sanitize_coord(floatval($data['Y']));
            $woeid = intval($data['woe_id']);
            $gnid = intval($data['gn_id']);
            $qsid = intval($data['qs_id']);

            // error_log(var_export($data, true));

            if ($woeid == 0) {
                // error_log('Missing woeid for current entry; skipping');
                continue;
            }

            if ($lon == 0.0 && $lat = 0.0) {
                // error_log("Missing coordinates for $woeid; skipping");
                continue;
            }

            $concordance = array($woeid, $gnid, $qsid);
            fwrite($fp, implode("\t", $concordance) . PHP_EOL);

            $select->bindValue(':woeid', $woeid);
            if ($select->execute()) {
                $ret = $select->fetchAll(\PDO::FETCH_ASSOC);
                if ($ret !== false) {
                    if (empty($ret)) {
                        // error_log("Inserting new coords for $woeid");
                        $insert->bindValue(':woeid', $woeid);
                        $insert->bindValue(':lon', $lon);
                        $insert->bindValue(':lat', $lat);
                        $insert->bindValue(':swlon', $swlon);
                        $insert->bindValue(':swlat', $swlat);
                        $insert->bindValue(':nelon', $nelon);
                        $insert->bindValue(':nelat', $nelat);
                        if (!$insert->execute()) {
                            error_log('update execute() failed: ' . var_export($db->errorInfo(), true));
                        }
                    }
                    else {
                        // error_log(var_export($ret, true));

                        $old_lon = floatval($ret[0]['lon']);
                        $old_lat = floatval($ret[0]['lat']);

                        if ($old_lon == 0.0 && $old_lat == 0.0) {
                            // error_log("Updating lat/lon for $woeid");
                            $update->bindValue(':woeid', $woeid);
                            $update->bindValue(':lon', $lon);
                            $update->bindValue(':lat', $lat);
                            if (!$update->execute()) {
                                error_log('insert execute() failed: ' . var_export($db->errorInfo(), true));
                            }
                        }
                        // else {
                        //     error_log("$woeid already has coordinates ($old_lon,$old_lat); skipping");
                        // }
                    }
                }
                else {
                    error_log('select fetchAll() failed: ' . var_export($db->errorInfo(), true));
                }
            }
            else {
                error_log('select execute() failed: ' . var_export($db->errorInfo(), true));
            }

            $this->show_status($row, $total);
        }

        fflush($fp);
        fclose($fp);

        $this->log("Pre-cached $total gazetteer entries");
    }

    private function task_geocode() {
        $db = $this->cache->get_cache();

        $sql = 'SELECT COUNT(DISTINCT woeid) FROM places;';
        $query = $db->prepare($sql);
        if (!$query) {
            error_log($sql);
            error_log('prepare failed: ' . var_export($db->errorInfo(), true));
            exit;
        }
        if (!$query->execute()) {
            error_log($sql);
            error_log('execute failed: ' . var_export($db->errorInfo(), true));
            exit;
        }
        $result = $query->fetch(\PDO::FETCH_ASSOC);
        $count = $result['COUNT(DISTINCT woeid)'];

        $sql = 'SELECT * FROM places;';
        $iter = $db->prepare($sql);
        if (!$iter) {
            error_log($sql);
            error_log('prepare failed: ' . var_export($db->errorInfo(), true));
            exit;
        }

        if (!$iter->execute()) {
            error_log($sql);
            error_log('execute failed: ' . var_export($db->errorInfo(), true));
            exit;
        }

        error_log("Checking and backfilling coordinates for $count woeids");
        $row = 0;
        while (($place = $iter->fetch(\PDO::FETCH_ASSOC)) !== false) {
            $row++;
            $woeid = intval($place['woeid']);
            $lon = floatval($place['lon']);
            $lat = floatval($place['lat']);
            $swlon = floatval($place['swlon']);
            $swlat = floatval($place['swlat']);
            $nelon = floatval($place['nelon']);
            $nelat = floatval($place['nelat']);
            $placetype = intval($place['placetype']);
            $name = $place['name'];

            // Skip backfilled empty placeholders ($placetype == 0)
            if ($placetype !== 0) {
                $has_centroid = ($lon !== 0.0 && $lat !== 0.0);
                $has_bounds = ($swlon !== 0.0 && $swlat !== 0.0 && $nelon == 0.0 && $nelat !== 0.0);

                if (!$has_centroid || !$has_bounds) {
                    $coords = $this->cache->get_coords($woeid);
                    if ($coords !== NULL) {
                        $lon = floatval($coords['lon']);
                        $lat = floatval($coords['lat']);
                        $swlon = floatval($coords['swlon']);
                        $swlat = floatval($coords['swlat']);
                        $nelon = floatval($coords['nelon']);
                        $nelat = floatval($coords['nelat']);

                        $has_centroid = ($lon !== 0.0 && $lat !== 0.0);
                        $has_bounds = ($swlon !== 0.0 && $swlat !== 0.0 && $nelon !== 0.0 && $nelat !== 0.0);
                    }
                }

                if (!$has_centroid || !$has_bounds) {
                    error_log($name . ', ' . $woeid . ': centroid: ' . ($has_centroid ? 'yes': 'no') . ' bounds: ' . ($has_bounds ? 'yes' : 'no'));
                    $admins = $this->cache->get_admins($woeid);
                    if ($admins !== NULL) {
                        error_log(var_export($admins, true));
                    }
                }
            }

            $this->show_status($row, $count);
        }
        error_log("Finished checking and backfilling coordinates for $count woeids");
    }

    private function task_coords() {
        $db = $this->cache->get_cache();

        $coords_file = $this->path . '/' . 'geoplanet_coords_updated_' . $this->version . '.tsv';

        if (!$fp = fopen($coords_file, 'w')) {
            error_log("Cannot open updated coordinates file $coords_file");
            exit;
        }

        $header = array('WOE_ID', 'Name', 'PlaceType', 'PlaceTypeID', 'Lat', 'Lon', 'SW_Lat', 'SW_Lon', 'NE_Lat', 'NE_Lon', 'ISO');
        fwrite($fp, implode("\t", $header). PHP_EOL);

        $sql = 'SELECT COUNT(DISTINCT woeid) FROM coords;';
        $query = $db->query($sql);
        if ($query === false) {
            throw new \Exception(sprintf('%s: PDO::query() (%s)', __FUNCTION__, $this->get_cache_error()));
        }
        $result = $query->fetch(\PDO::FETCH_ASSOC);
        $count = $result['COUNT(DISTINCT woeid)'];
        $this->log("Exporting updated coordinates for $count woeids");

        $sql = 'SELECT * FROM coords;';
        $iter = $db->query($sql);
        $row = 0;

        while (($coords = $iter->fetch(\PDO::FETCH_ASSOC)) !== false) {
            $row++;

            $woeid = intval($coords['woeid']);
            $place = $this->cache->get_woeid($woeid);
            if (NULL === $place) {
                throw new \Exception("Cannot find place record for $woeid");
            }

            $data = array(
                    $woeid,
                    $place['name'],
                    $place['placetypename'],
                    $place['placetype'],
                    $coords['lat'],
                    $coords['lon'],
                    $coords['swlat'],
                    $coords['swlon'],
                    $coords['nelat'],
                    $coords['nelon'],
                    $place['iso']
            );

            fwrite($fp, implode("\t", $data) . PHP_EOL);
            $this->show_status($row, $count);
        }

        fflush($fp);
        fclose($fp);

        error_log("Exported updated coordinates for $count woeids");
    }

    private function sanitize_coord($coord) {
        if ($coord == '\N' || $coord == '\n' || $coord == NULL || !is_numeric($coord) || $coord == '') {
            $coord = 0.0;
        }

        return $coord;
    }
}

$shortopts = "vq:p:t:";
$longopts = array(
    "verbose",
    "quattroshapes:",
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

if (isset($options['q'])) {
    $quattroshapes = $options['q'];
}
else if (isset($options['quattroshapes'])) {
    $quattroshapes = $options['quattroshapes'];
}
else {
    echo "Missing path to Quattroshapes data\n";
    exit;
}

if (isset($options['t'])) {
    $task = $options['t'];
}
else if (isset($options['task'])) {
    $task = $options['task'];
}

$updater = new SourceUpdate($path, $quattroshapes, $verbose, $task);
$updater->run();

?>
