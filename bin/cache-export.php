#!/usr/bin/env php
<?php

require_once '../lib/task-runner.php';
require_once '../lib/timer.php';
require_once '../lib/cache-utils.php';
require_once '../lib/placetypes.php';
require_once '../lib/geometry.php';

Class CacheExport extends Woeplanet\TaskRunner {
    const RUN_TASK = 'run';
    const EXPORT_TASK = 'export';
    const TEST_TASK = 'test';

    const EXPORT_GEOJSON = 'geojson';
    const EXPORT_POSTGIS = 'postgis';
    const EXPORT_ELASTICSEARCH = 'elasticsearch';

    private $config;
    private $cache;
    private $dest;
    private $task;
    private $placetype;

    public function __construct($config) {
        parent::__construct($config['verbose']);
        $this->config = $config;

        $this->cache = new \Woeplanet\CacheUtils($config['input']);

        $this->task = $config['task'];
        $this->placetype = $config['placetype'];
        $this->dest = $config['destination'];
        
        $this->tasks = array(
            self::RUN_TASK,
            self::EXPORT_TASK,
            self::TEST_TASK
        );

        $this->timer = new Woeplanet\Timer($this->tasks);
    }

    public function run() {
        $this->timer->elapsed(self::RUN_TASK);

        if (!empty($this->task)) {
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

    private function task_export() {
        $export = '';
        switch ($this->config['output']) {
            case self::EXPORT_GEOJSON:
            case self::EXPORT_POSTGIS:
            case self::EXPORT_ELASTICSEARCH:
                $export = $this->config['output'];
                break;
            default:
                throw new \Exception('Unrecognised output type ' . $this->config['output']);
                break;
        }

        $func = "export_$export";

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

                $this->$func($doc, $tag);

                // $woeid = intval($doc['woeid']);
                // $path = $this->make_export_path($tag, $woeid);
                // if (!is_dir($path)) {
                //     mkdir($path, 0777, true);
                // }
                // $filename = $path . DIRECTORY_SEPARATOR . strval($woeid) . '.geojson';
                // $json = $this->format_geojson($doc);
                // $ret = file_put_contents($filename, $json);
                $this->show_status($files, $total);
            }
            $query->closeCursor();
        }

    }

    private function export_geojson($doc, $tag) {
        $woeid = intval($doc['woeid']);
        $path = $this->make_export_path($tag, $woeid);
        if (!is_dir($path)) {
            mkdir($path, 0777, true);
        }
        $filename = $path . DIRECTORY_SEPARATOR . strval($woeid) . '.geojson';
        $json = $this->format_geojson($doc);
        $ret = file_put_contents($filename, $json);
    }

    private function export_postgis($doc, $tag) {
        $this->log('export_postgis');
    }

    private function export_elasticsearch($doc, $tag) {
        $this->log('export_elasticsearch');
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
                $json = $this->format_geojson($doc);
                $ret = file_put_contents($filename, $json);
                $this->show_status($files, $total);
            }
            $query->closeCursor();
        }
    }

    private function format_geojson($doc) {
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

// -v --verbose
// -t --task 'task-name'
// -i --input 'input-cache-name'
// -o --output 'elasticsearch|geojson|postgis'
// -p --placetype 'placetype-tag'
// -d --destination 'path' (--output geojson only)
// -c --credentials 'credentials' (--output elasticsearch and --output postgis only)

$shortopts = 'vt:i:o:p:d:c:';
$longopts = array('verbose', 'task:', 'input:', 'output:', 'placetype:', 'destination:', 'credentials:');
$config = array(
    'verbose' => false,
    'task' => NULL,
    'input' => 'geoplanet_cache.sqlite3',
    'output' => NULL,
    'placetype' => NULL,
    'destination' => './geojson',
    'credentials' => NULL
);
$output = array('elasticsearch', 'geojson', 'postgis');

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

if (isset($options['o'])) {
    $config['output'] = $options['o'];
}
else if (isset($options['output'])) {
    $config['output'] = $options['output'];
}

if (isset($options['p'])) {
    $config['placetype'] = $options['p'];
}
elseif (isset($options['placetype'])) {
    $config['placetype'] = $options['placetype'];
}

if (isset($options['d'])) {
    $config['destination'] = $options['d'];
}
else if (isset($options['destination'])) {
    $config['destination'] = $options['destination'];
}

if (isset($options['c'])) {
    $config['credentials'] = $options['c'];
}
else if (isset($options['credentials'])) {
    $config['credentials'] = $options['credentials'];
}

foreach ($config as $key => $value) {
    if (is_string($value)) {
        $config[$key] = strtolower($value);
    }
}

if (!empty($config['placetype'])) {
    $placetypes = new \Woeplanet\PlaceTypes();
    $pt = $placetypes->get_by_tag($config['placetype']);
    if (!$pt['found']) {
        echo "Invalid or unrecognised placetype " . $config['placetype'] . "\n";
        exit;
    }
}

if (empty($config['output'])) {
    echo "Missing required output type\n";
    exit;
}
else {
    if (!in_array($config['output'], $output)) {
        echo "Invalid or unrecognised output type " . $config['output'] . "\n";
        exit;
    }
}

if ($config['output'] === 'geojson' && empty($config['destination'])) {
    echo "Output type 'geojson' needs an output destination\n";
    exit;
}
else if (($config['output'] === 'postgis' || $config['output'] === 'elasticsearch') && empty($config['credentials'])) {
    echo "Output types 'postgis' and 'elasticsearch' need credentials\n";
    exit;
}

$exporter = new CacheExport($config);
$exporter->run();

?>
