#!/usr/bin/env php
<?php

require_once 'runner.php';
require_once 'timer.php';
require_once 'vendor/autoload.php';

class FlickrShapesImport extends Woeplanet\Runner {
    const RUN_STAGE = 'run';
    const SETUP_STAGE = 'setup';
    const SHAPES_STAGE = 'shapes';
    const TEST_STAGE = 'test';

    private $path;

    private $source;
    private $version;
    private $timestamp;
    private $history;

    private $files;
    private $stage;
    private $stages;
    private $timer;

    public function __construct($path, $elasticsearch, $verbose=FALSE, $stage=NULL) {
        parent::__construct($elasticsearch, $verbose);

        $this->path = $path;
        $this->stage = $stage;

        $this->stages = array(
            self::RUN_STAGE,
            self::SETUP_STAGE,
            self::TEST_STAGE
        );

        $match = array();
        $pattern = '/(.+)_([\d\.]+)$/';
        $ret = preg_match($pattern, basename($this->path), $match);
        if ($ret === 1) {
            $this->source = $match[1];
            $this->version = $match[2];
            $this->timestamp = time();
            $this->history = array(
                'source' => sprintf('%s %s', $this->source, $this->version),
                'timestamp' => (int) $this->timestamp
            );
        }

        else {
            $this->log("Can't get source and version from path $this->path");
        }

        $this->files = array();
        //$this->stages = array();

        $dir = opendir($this->path);
        while (false !== ($entry = readdir($dir))) {
            if ($entry === '.' || $entry === '..')
                continue;

            $pattern = '/(.*)flickr_shapes_(.+)\.geojson$/';
            $ret = preg_match($pattern, $entry, $match);
            if ($ret === 1) {
                //var_dump($match);
                $this->files[$entry] = array(
                    'path' => $this->path . DIRECTORY_SEPARATOR . $entry,
                    'stage' => $match[2]
                );
                $this->stages[] = $match[2];
            }

            else {
                $this->log("Can't get shape type from $entry");
            }

            $this->timer = new Woeplanet\Timer($this->stages);
        }
    }

    public function run() {
        $this->timer->elapsed(self::RUN_STAGE);

        if (isset($this->stage) && $stage != self::RUN_STAGE) {
            $this->time->elapsed($this->stage);

            $func = "index_$this->stage";
            if (method_exists($this, $func)) {
                $this->$func();
            }
            else {
                $this->import_shape($this->stage);
            }

            $elapsed = $this->timer->seconds_to_time($this->timer->elapsed($this->stage));
            $this->log("Completed stage $this->stage in $elapsed");
        }

        else {
            foreach ($this->stages as $stage) {
                $this->log("Dispatching $stage");
                if ($stage != self::RUN_STAGE && $stage != self::TEST_STAGE) {
                    $this->timer->elapsed($stage);

                    $func = "index_$stage";

                    if (method_exists($this, $func)) {
                        $this->$func();
                    }
                    else {
                        $this->import_shape($stage);
                    }

                    $elapsed = $this->timer->seconds_to_time($this->timer->elapsed($stage));
                    $this->log("Completed stage $stage in $elapsed");
                }
            }
        }

        $elapsed = $this->timer->seconds_to_time($this->timer->elapsed(self::RUN_STAGE));
        $this->log("Completed in $elapsed");

        // foreach ($this->files as $entry => $file) {
        //     $this->log('Entry ' . $entry);
        //     $this->log('File ' . var_export($file, true));
        //     $this->import_flickr_shapes($file['path'], $file['stage']);
        // }
    }

    private function index_setup() {
        $this->log('index_setup');
    }

    private function index_test() {
        $this->log('index_test');
    }

    private function import_shape($stage) {
        $this->log("import shape - $stage");
    }

    private function import_flickr_shapes($path, $stage) {
        $this->timer->elapsed($stage);

        $data = file_get_contents($path);
        if ($data !== FALSE) {
            // error_log("Type: " . gettype($data));
            // error_log("Len: " . strlen($data));
            // error_log("DATA++");
            //error_log(var_export($data, true));
            // error_log("DATA--");
            $json =$this->decode_geojson($data);
            if ($json === NULL) {
                $err = error_get_last();
                $this->log("Failed to decode JSON for $path, " . $err['message']);
            }
            //error_log(var_export($json, true));

            $features = $json['features'];
            error_log('Found ' . count($features) . ' features');
            exit;
        }
        else {
            $err = error_get_last();
            $this->log("Failed to get contents of $path, " . $err['message']);
        }
        $elapsed = $this->timer->seconds_to_time($this->timer->elapsed($stage));
        $this->log("Completed $stage import in $elapsed");
    }

    private function decode_geojson($json) {
        $assoc = TRUE;
        return json_decode($this->trim_commas(utf8_encode($json)), $assoc);
    }

    function trim_commas($json)
    {
        $json = preg_replace('/,\s*([\]}])/m', '$1', $json);
        return $json;
    }
}

$shortopts = 've:p:s:';
$longopts = array(
    'verbose',
    'elasticsearch:',
    'path:',
    'stage:'
);

$verbose = false;
$elasticsearch = 'http://localhost:9200';
$path = NULL;
$stage = NULL;

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

if (isset($options['s'])) {
    $stage = $options['s'];
}
else if (isset($options['stage'])) {
    $stage = $options['stage'];
}

if (isset($options['p'])) {
    $path = $options['p'];
}
else if (isset($options['path'])) {
    $path = $options['path'];
}
else {
    echo "Missing path to Flickr shapes\n";
    exit;
}

$import = new FlickrShapesImport($path, $elasticsearch, $verbose, $stage);
$import->run();

?>
