<?php

abstract class WoePlanetBase {
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
		$this->es =new Elasticsearch\Client($params);
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
		//error_log("get_woeid: $woeid");
		$params = array(
			'index' => self::INDEX,
			'type' => self::PLACES_TYPE,
			'id' => $woeid,
			'ignore' => 404
		);
		$doc = $this->es->get($params);
		//error_log('get_woeid: $doc type = ' . gettype($doc));
		if (is_string($doc)) {
			// error_log('get_woeid: $doc type is string');
			// error_log('get_woeid: $doc: ' . $doc);
			$assoc = true;
			$doc = json_decode($doc, $assoc);
			// error_log('get_woeid: typeof $doc is now ' . gettype($doc));
		}
		// else {
		// 	error_log('get_woeid: $doc type is NOT string');
		// }
		return $doc;
	}


	public function log($message) {
		echo "$message\n";
	}

	public function logVerbose($message) {
		if ($this->verbose) {
			$this->logMessage($message);
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

class WoePlanetTimer {
	static $last;

	public function __construct($stages) {
		self::$last = array();
		foreach ($stages as $stage) {
			self::$last[$stage] = null;
		}
	}

	public function elapsed($stage) {
		$now = time();
		$elapsed = $now;
		if (self::$last[$stage] != null) {
			$elapsed = ($now - self::$last[$stage]);
		}

		self::$last[$stage] = $now;
		return $elapsed;
	}

	public function seconds_to_time($seconds) {
		$dtf = new DateTime("@0");
		$dtt = new DateTime("@$seconds");
		return $dtf->diff($dtt)->format('%h hours, %i mins, %s secs');
	}
}
?>
