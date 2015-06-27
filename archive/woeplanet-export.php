#!/usr/bin/env php
<?php

require 'vendor/autoload.php';

$shortopts = 've:w:';
$longopts = array(
	'verbose',
	'elasticsearch:',
	'woeplanet:'
);

$verbose = false;
$elasticsearch = 'http://localhost:9200';
$woeplanet = 'woeplanet-data';

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

if (isset($options['w'])) {
	$woeplanet = $options['w'];
}
else if (isset($options['woeplanet'])) {
	$woeplanet = $options['woeplanet'];
}

$exporter = new WoePlanetExporter($elasticsearch, $woeplanet, $verbose);
$exporter->run();

class WoePlanetExporter {
	const INDEX = "woeplanet";

	const PLACES_TYPE = "places";
	const ADMINS_TYPE = "admins";
	const PLACETYPES_TYPE = "placetype";
	const META_TYPE = 'meta';

	private $elasticsearch;
	private $woeplanet;
	private $verbose;
	private $instance;

	public function __construct($elasticsearch, $woeplanet, $verbose) {
		$this->elasticsearch = $elasticsearch;
		$this->woeplanet = $woeplanet;
		$this->verbose = $verbose;

		$params = array();
		$params['hosts'] = array($this->elasticsearch);
		$this->instance = new Elasticsearch\Client($params);
	}

	public function run() {
		$max_woeid = $this->get_max_woeid();
		if ($max_woeid === false) {

		}

		else {
			$woeid = 1;
			while ($woeid <= $max_woeid) {
				$path = $this->woeplanet . DIRECTORY_SEPARATOR . $this->woeid_to_path($woeid);
				$this->log($woeid . ': ' . $path);
				$this->make_path($path);
				//$doc = $this->get_by_woeid($woeid);
				//$this->show_status($woeid, $max_woeid);
				$woeid++;
			}
		}
	}

	private function get_max_woeid() {
		$params = array(
			'index' => self::INDEX,
			'type' => self::META_TYPE,
			'id' => 1
		);
		$doc = $this->instance->get($params);
		if ($doc['found']) {
			return (int)$doc['_source']['max_woeid'];
		}

		return false;
	}

	private function get_by_woeid($woeid) {
		$params = array(
			'index' => self::INDEX,
			'type' => self::PLACES_TYPE,
			'id' => $woeid,
			'ignore' => 404
		);
		$doc = $this->instance->get($params);
		if (is_string($doc)) {
			$doc = json_decode($doc);
		}

		return $doc;
	}

	private function woeid_to_path($woeid) {
		$id = $woeid;
		$path = array();

		while (strlen($id) > 3) {
			$element = substr($id, 0, 3);
			$path[] = $element;
			$id = substr($id, 3);
		}

		if (!empty($id)) {
			$path[] = $id;
		}

		return implode('/', $path);
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

	private function log($message) {
		echo "$message\n";
	}

	private function logVerbose($message) {
		if ($this->verbose) {
			$this->log($message);
		}
	}

}
?>
