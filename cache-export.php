#!/usr/bin/env php
<?php

require_once 'task-runner.php';
require_once 'timer.php';
require_once 'cache-utils.php';
require_once 'placetypes.php';
require_once 'geometry.php';

Class CacheExport extends Woeplanet\TaskRunner {
    const RUN_TASK = 'run';
    const GEOJSON_TASK = 'geojson';
    const TEST_TASK = 'test';

    private $cache;
    private $dest;
    private $task;
    private $placetype;

    public function __construct($verbose, $dest, $cache, $placetype=NULL, $task=NULL) {
        parent::__construct($verbose);

        $this->cache = new \Woeplanet\CacheUtils($cache);

        if ($task !== NULL) {
            $task = strtolower($task);
        }
        $this->task = $task;
        $this->dest = $dest;
        if ($placetype !== NULL) {
            $this->placetype = $placetype;
        }

        $this->tasks = array(
            self::RUN_TASK,
            self::GEOJSON_TASK,
            self::TEST_TASK
        );

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
                    $this->log("Completed stage $task in $elapsed");
                }
            }
        }

        $elapsed = $this->timer->seconds_to_time($this->timer->elapsed(self::RUN_TASK));
        $this->log("Completed in $elapsed");
    }

    private function task_geojson() {
        $placetypes = new \Woeplanet\PlaceTypes();
        $pts = $placetypes->get();
        $db = $this->cache->get_cache();

        $this->log('Creating places(placetype) index');
        $sql = 'CREATE INDEX IF NOT EXISTS places_by_placetype ON places(placetype);';
        if ($db->exec($sql) === FALSE) {
            throw new \Exception($sql . ':' . $this->cache->get_cache_error());
        }

        $sql = 'SELECT COUNT(DISTINCT woeid) FROM places WHERE placetype = :placetype;';
        $count = $db->prepare($sql);

        $sql = 'SELECT * FROM places WHERE placetype = :placetype;';
        $query = $db->prepare($sql);

        foreach ($pts as $placetype) {
            $id = intval($placetype['id']);
            $tag = $placetype['tag'];

            if ($this->placetype !== NULL && $tag !== $this->placetype) {
                continue;
            }

            $count->bindValue(':placetype', $id);
            $count->execute();
            $result = $count->fetch(\PDO::FETCH_ASSOC);
            $total = $result['COUNT(DISTINCT woeid)'];
            $count->closeCursor();
            $this->log('Exporting ' . $total . ' woeids for placetype ' . $placetype['name']);

            $query->bindValue(':placetype', $id);
            $query->execute();

            $files = 0;
            while (($doc = $query->fetch(\PDO::FETCH_ASSOC)) !== false) {
                $files++;
                $doc = $this->cache->unpack_place($doc);
                $woeid = intval($doc['woeid']);
                $path = $this->make_export_path($tag, $woeid);
                if (!is_dir($path)) {
                    mkdir($path, 0777, true);
                }
                $filename = $path . DIRECTORY_SEPARATOR . strval($woeid) . '.geojson';
                $json = $this->geojson_export($doc);
                $ret = file_put_contents($filename, $json);
                $this->show_status($files, $total);
            }
            $query->closeCursor();
        }
    }

    private function geojson_export($doc) {
        $woeid = intval($doc['woeid']);
        $centroid = NULL;
        $bounds = NULL;
        // $geometry = array();
        // $bbox = array();
        // $bounds = array();
        $properties = array();

        if (isset($doc['lat']) && !empty($doc['lat']) && isset($doc['lon']) && !empty($doc['lon'])) {
            $centroid = new Woeplanet\Centroid(floatval($doc['lon']), floatval($doc['lat']));
            // $geometry = array(
            //     'type' => 'Point',
            //     'coordinates' => array(floatval($doc['lon']), floatval($doc['lat']))
            // );
        }

        if ((isset($doc['swlat']) && !empty($doc['swlat']) && isset($doc['swlon']) && !empty($doc['swlon'])) &&
                (isset($doc['nelat']) && !empty($doc['nelat']) && isset($doc['nelon']) && !empty($doc['nelon']))) {
            $bounds = new Woeplanet\BoundingBox(floatval($doc['swlon']), floatval($doc['swlat']), floatval($doc['nelon']), floatval($doc['nelat']));
            // $bbox = array(floatval($doc['nelon']), floatval($doc['nelat']), floatval($doc['swlon']), floatval($doc['swlat']));
            // $bounds = array(
            //     'type' => 'Polygon',
            //     'coordinates' => array(
            //         array(
            //             array(floatval($doc['nelon']), floatval($doc['nelat'])),
            //             array(floatval($doc['swlon']), floatval($doc['swlat'])),
            //             array(floatval($doc['nelon']), floatval($doc['nelat']))
            //
            //         )
            //     )
            // );
        }

        $properties = array(
            'woeid' => $woeid,
            'name' => $doc['name'],
            'iso' => $doc['iso'],
            'lang' => $doc['lang'],
            'placetype' => intval($doc['placetype']),
            'placetypename' => $doc['placetypename'],
            'parent' => $doc['parent']
        );

        if (!empty($doc['adjacent'])) {
            $adjacent = array();
            foreach ($doc['adjacent'] as $adj) {
                $nvp = explode('=', $adj);
                $type = $nvp[0];
                $id = $nvp[1];

                if (isset($adjacent[$type])) {
                    $adjacent[$type][] = $id;
                }
                else {
                    $adjacent[$type] = array($id);
                }
            }
            $properties['adjacent'] = $adjacent;
        }

        if (!empty($doc['alias_q'])) {
            $properties['alias_q'] = $doc['alias_q'];
        }
        if (!empty($doc['alias_v'])) {
            $properties['alias_v'] = $doc['alias_v'];
        }
        if (!empty($doc['alias_a'])) {
            $properties['alias_a'] = $doc['alias_a'];
        }
        if (!empty($doc['alias_s'])) {
            $properties['alias_s'] = $doc['alias_s'];
        }
        if (!empty($doc['alias_p'])) {
            $properties['alias_p'] = $doc['alias_p'];
        }

        $admins = $this->cache->get_admins($woeid);
        if ($admins) {
            unset($admins['woeid']);
            $properties['admins'] = $admins;
        }

        if (!empty($doc['supercedes'])) {
            $properties['supercedes'] = intval($doc['supercedes']);
        }
        if (!empty($doc['superceded'])) {
            $properties['superceded'] = $doc['superceded'];
        }

        $properties['history'] = $doc['history'];

        $geojson = array('type' => 'Feature');
        if ($bounds && !$bounds->is_empty()) {
            $geojson['bbox'] = $bounds->to_bbox();
        }
        if ($centroid && !$centroid->is_empty()) {
            if (!empty($bounds)) {
                $geojson['geometry'] = array(
                    'type' => 'GeometryCollection',
                    'geometries' => array(
                        $centroid->to_geojson(),
                        $bounds->to_geojson()
                    )
                );
            }
            else {
                $geojson['geometry'] = $centroid->to_geojson();
            }
        }
        $geojson['properties'] = $properties;

        $json = json_encode($geojson, JSON_UNESCAPED_UNICODE);
        // error_log(var_export($json, true));
        return $json;
    }

    private function make_export_path($placetype, $woeid) {
        $path = array($this->dest, $placetype);
        $id = strval($woeid);

        while (strlen($id) > 3) {
            $element = substr($id, 0, 3);
            $path[] = $element;
            $id = substr($id, 3);
        }

        if (!empty($id)) {
            $path[] = $id;
        }

        return implode(DIRECTORY_SEPARATOR, $path);
    }

    private function task_test() {
        $woeid = 44418;
        $doc = $this->cache->get_woeid($woeid);
        $this->geojson_export($doc);
    }
}

$verbose = true;
$task = NULL;
$dest = NULL;
$placetype = NULL;
$cache = 'geoplanet_cache.sqlite3';

$shortopts = 'vt:d:c:p:';
$longopts = array('verbose', 'task:', 'dest:', 'cache:', 'placetype:');
$options = getopt($shortopts, $longopts);

if (isset($options['v']) || isset($options['verbose'])) {
    $verbose = true;
}

if (isset($options['t'])) {
    $task = $options['t'];
}
else if (isset($options['task'])) {
    $task = $options['task'];
}

if (isset($options['d'])) {
    $dest = $options['d'];
}
else if (isset($options['dest'])) {
    $dest = $options['dest'];
}
else {
    echo "Missing path to export destination directory\n";
    exit;
}

if (isset($options['p'])) {
    $placetype = $options['p'];
}
elseif (isset($options['placetype'])) {
    $placetype = $options['placetype'];
}

if (isset($options['c'])) {
    $cache = $options['c'];
}
else if (isset($options['cache'])) {
    $cache = $options['cache'];
}

$exporter = new CacheExport($verbose, $dest, $cache, $placetype, $task);
$exporter->run();

?>
