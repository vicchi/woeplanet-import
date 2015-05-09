<?php

namespace Woeplanet;

abstract class Runner {
	const INDEX = "woeplanet";

	const PLACES_TYPE = "places";
	const ADMINS_TYPE = "admins";
	const PLACETYPES_TYPE = "placetype";
	const META_TYPE = 'meta';

	protected $verbose;
	protected $elasticsearch;
	protected $es;

	public function __construct($elasticsearch, $verbose=false) {
		$this->elasticsearch = $elasticsearch;
		$this->verbose = $verbose;

		$params = array(
			'hosts' => array($this->elasticsearch)
		);
		$this->es =new \Elasticsearch\Client($params);
	}

	abstract public function run();

	protected function get_max_woeid() {
		$params = array(
			'index' => self::INDEX,
			'type' => self::META_TYPE,
			'id' => 1
		);
		$doc = $this->es->get($params);
		if ($doc['found']) {
			return (int)$doc['_source']['max_woeid'];
		}

		throw new Exception('Cannot find max_woeid type');
	}

	protected function get_woeid($woeid) {
		$params = array(
			'index' => self::INDEX,
			'type' => self::PLACES_TYPE,
			'id' => $woeid,
			'ignore' => 404
		);
		$doc = $this->es->get($params);
		if (is_string($doc)) {
			$assoc = true;
			$doc = json_decode($doc, $assoc);
		}

		return $doc;
	}

	protected function get_admin($woeid) {
		$params = array(
			'index' => self::INDEX,
			'type' => self::ADMINS_TYPE,
			'id' => $woeid,
			'ignore' => 404
		);
		$doc = $this->es->get($params);
		if (is_string($doc)) {
			$assoc = true;
			$doc = json_decode($doc, $assoc);
		}

		return $doc;
	}

	protected function get_meta() {
		$params = array(
			'index' => self::INDEX,
			'type' => self::META_TYPE,
			'id' => 1
		);
		$doc = $this->es->get($params);
		if (is_string($doc)) {
			$assoc = true;
			$doc = json_decode($doc, $assoc);
		}

		return $doc;
	}

	protected function refresh_meta($woeid) {
		$meta = $this->get_meta();
		if ((int)$woeid > (int)$meta['_source']['max_woeid']) {
			$params = array(
				'body' => array(
					'max_woeid' => (int)$woeid
				),
				'index' => self::INDEX,
				'type' => self::META_TYPE,
				'id' => 1,
				'refresh' => true
			);
			$this->es->index($params);
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
