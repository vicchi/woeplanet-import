<?php

namespace Woeplanet;

require_once 'vendor/autoload.php';

abstract class Runner {
	const DATABASE = "woeplanet";

	const PLACES_TYPE = "places";
	const ADMINS_TYPE = "admins";
	const PLACETYPES_TYPE = "placetypes";
	const META_TYPE = 'meta';

	protected $verbose;
	protected $server;
	protected $client;
	protected $db;
	protected $collections;

	public function __construct($server, $verbose=false) {
		$this->server = $server;
		$this->verbose = $verbose;
		$this->client = new \Elasticsearch\Client(array('hosts' => array($server)));
	}

	abstract public function run();

	// protected function get_max_woeid() {
	// 	$query = array('_id' => 1);
	// 	$meta = $this->collections[self::META_COLLECTION]->findOne($query);
	// 	if ($meta !== NULL) {
	// 		return (int)$meta['max_woeid'];
	// 	}
	//
	// 	throw new Exception('Cannot find max_woeid type');
	// }

	protected function get_woeid($woeid) {
		$params = array(
			'index' => self::DATABASE,
			'type' => self::PLACES_TYPE,
			'id' => $woeid,
			'ignore' => 404
		);
		$doc = $this->client->get($params);
		if (is_string($doc)) {
			$assoc = true;
			$doc = json_decode($doc, $assoc);
		}

		if (isset($doc['found']) && $doc['found']) {
			return $doc['_source'];
		}
		return NULL;
	}

	protected function get_admin($woeid) {
		$params = array(
			'index' => self::DATABASE,
			'type' => self::ADMINS_TYPE,
			'id' => $woeid,
			'ignore' => 404
		);
		$doc = $this->client->get($params);
		if (is_string($doc)) {
			$assoc = true;
			$doc = json_decode($doc, $assoc);
		}

		if ($doc['found']) {
			return $doc['_source'];
		}
		return NULL;
	}

	protected function get_meta() {
		$params = array(
			'index' => self::DATABASE,
			'type' => self::META_TYPE,
			'id' => 1
		);
		$doc = $this->client->get($params);
		if (is_string($doc)) {
			$assoc = true;
			$doc = json_decode($doc, $assoc);
		}

		if ($doc['found']) {
			return $doc['_source'];
		}
		return NULL;
	}

	protected function refresh_meta($woeid) {
		$meta = $this->get_meta();

		if (NULL === $meta) {
			$meta = array(
				'max_woeid' => (int)$woeid
			);

			$params = array(
				'body' => $meta,
				'index' => self::DATABASE,
				'type' => self::META_TYPE,
				'id' => 1
			);
			$this->client->index($params);
		}

		else if ((int)$woeid > (int)$meta['max_woeid']) {
			$meta['max_woeid'] = (int)$woeid;

			$params = array(
				'body' => $meta,
				'index' => self::DATABASE,
				'type' => self::META_TYPE,
				'id' => 1
			);
			$this->client->index($params);
		}
	}

	public function log($message) {
		echo "$message\n";
	}

	public function logVerbose($message) {
		if ($this->verbose) {
			$this->log($message);
		}
	}

	// Thanks to Brian Moon for this - http://brian.moonspot.net/php-progress-bar
	public static function show_status($done, $total, $size=30) {
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
}

?>
