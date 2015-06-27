#!/usr/bin/env php
<?php

require_once 'woeplanet-base.php';
require_once 'vendor/autoload.php';

class WoePlanetExport extends WoePlanetBase {
    private $path;
    private $timer;

    public function __construct($elasticsearch, $path, $verbose=false) {
        parent::__construct($elasticsearch, $verbose);

        $this->path = $path;

        $stages = array('export');
        $this->timer = new WoePlanetTimer($stages);
    }

    public function run() {
        $this->timer->elapsed('export');
        $max_woeid = $this->get_max_woeid();

        $woeid = 1;
        while ($woeid <= $max_woeid) {
            $path = $this->path . DIRECTORY_SEPARATOR . $this->woeid_to_path($woeid);
            //$this->log($woeid . ': ' . $path);
            $this->make_path($path);

            $doc = $this->get_woeid($woeid);
            //error_log('run: $doc: ' . var_export($doc, true));
            if ($doc['found']) {
                $geojson = json_encode($doc['_source'], JSON_PRETTY_PRINT);
                $file = $path . DIRECTORY_SEPARATOR . $woeid . '.geojson';
                file_put_contents($file, $geojson);
                //error_log('run: File: ' . $file);
                //var_dump($geojson);
            }
            //var_dump($doc);
            //exit;
            $this->show_status($woeid, $max_woeid);
            $woeid++;
        }
        $woeid--;

        $this->log("\nExported $woeid of $max_woeid WOEIDs");

        $elapsed = $this->timer->seconds_to_time($this->timer->elapsed('export'));
        $this->log("Completed exporting WOEIDs in $elapsed");

    }

    private function woeid_to_path($woeid) {
        $id = $woeid;
        $path = array();

        while (strlen($id)> 3) {
            $element = substr($id, 0, 3);
            $path[] = $element;
            $id = substr($id, 3);
        }

        if (!empty($id)) {
            $path[] = $id;
        }

        return implode(DIRECTORY_SEPARATOR, $path);
    }

    // http://edmondscommerce.github.io/php/php-recursive-create-path-if-not-exists.html
    private function make_path($path, $mode=0777, $isfile=false) {
        if($isfile) {
            $path = substr($path, 0, strrpos($path, '/'));
        }

        // Check if directory already exists
        if (is_dir($path) || empty($path)) {
            return true;
        }

        // Ensure a file does not already exist with the same name
        $path = str_replace(array('/', '\\'), DIRECTORY_SEPARATOR, $path);
        if (is_file($path)) {
            trigger_error('mkdir() File exists', E_USER_WARNING);
            return false;
        }

        // Crawl up the directory tree

        $next_path = substr($path, 0, strrpos($path, DIRECTORY_SEPARATOR));

        if ($this->make_path($next_path, $mode, $isfile)) {
            if (!file_exists($path)) {
                return mkdir($path, $mode);
            }
        }

        return false;
    }
}

$shortopts = 've:p:';
$longopts = array(
    'verbose',
    'elasticsearch:',
    'path:'
);

$verbose = false;
$elasticsearch = 'http://localhost:9200';
$path = 'woeplanet-data';

$options = getopt($shortopts, $longopts);

if (isset($options['v']) || isset($options['verbose'])) {
    $verbose = true;
}

if (isset($options['e'])) {
    $elasticsearch = $options['e'];
}
else if (isset($options['elasticsearch'])) {
    $elasticsearch = $options['elasticsearch'];
}

if (isset($options['p'])) {
    $path = $options['p'];
}
else if (isset($options['path'])) {
    $path = $options['path'];
}

$export = new WoePlanetExport($elasticsearch, $path, $verbose);
$export->run();

?>
