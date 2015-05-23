<?php

namespace Woeplanet;

abstract class Runner {
	const DATABASE = "woeplanet";

	const PLACES_COLLECTION = "places";
	const ADMINS_COLLECTION = "admins";
	const PLACETYPES_COLLECTION = "placetypes";
	const META_COLLECTION = 'meta';

	protected $verbose;
	protected $server;
	protected $client;
	protected $db;
	protected $collections;

	public function __construct($server, $verbose=false) {
		$this->server = $server;
		$this->verbose = $verbose;
		$this->client = new \MongoClient($server);

		$this->db = $this->client->selectDB(self::DATABASE);
		$this->collections = array();
		$this->collections[self::PLACES_COLLECTION] = $this->db->selectCollection(self::PLACES_COLLECTION);
		$this->collections[self::ADMINS_COLLECTION] = $this->db->selectCollection(self::ADMINS_COLLECTION);
		$this->collections[self::PLACETYPES_COLLECTION] = $this->db->selectCollection(self::PLACETYPES_COLLECTION);
		$this->collections[self::META_COLLECTION] = $this->db->selectCollection(self::META_COLLECTION);
	}

	abstract public function run();

	protected function get_max_woeid() {
		$query = array('_id' => 1);
		$meta = $this->collections[self::META_COLLECTION]->findOne($query);
		if ($meta !== NULL) {
			return (int)$meta['max_woeid'];
		}

		throw new Exception('Cannot find max_woeid type');
	}

	protected function get_woeid($woeid) {
		// error_log('get_woeid: ' . $woeid);
		$query = array(
			'_id' => (int)$woeid
			// '_id' => new \MongoId($woeid)
		);

		// $doc = $this->collections[self::PLACES_COLLECTION]->findOne($query);
		// error_log(var_export($doc, true));
		return $this->collections[self::PLACES_COLLECTION]->findOne($query);
		// $params = array(
		// 	'index' => self::DATABASE,
		// 	'type' => self::PLACES_COLLECTION,
		// 	'id' => $woeid,
		// 	'ignore' => 404
		// );
		// $doc = $this->client->get($params);
		// if (is_string($doc)) {
		// 	$assoc = true;
		// 	$doc = json_decode($doc, $assoc);
		// }
		//
		// return $doc;
	}

	protected function get_admin($woeid) {
		$params = array(
			'index' => self::DATABASE,
			'type' => self::ADMINS_COLLECTION,
			'id' => $woeid,
			'ignore' => 404
		);
		$doc = $this->client->get($params);
		if (is_string($doc)) {
			$assoc = true;
			$doc = json_decode($doc, $assoc);
		}

		return $doc;
	}

	protected function get_meta() {
		$query = array(
			'_id' => 1
		);
		return $this->collections[self::META_COLLECTION]->findOne($query);
		//
		// $params = array(
		// 	'index' => self::DATABASE,
		// 	'type' => self::META_COLLECTION,
		// 	'id' => 1
		// );
		// $doc = $this->client->get($params);
		// if (is_string($doc)) {
		// 	$assoc = true;
		// 	$doc = json_decode($doc, $assoc);
		// }
		//
		// return $doc;
	}

	protected function refresh_meta($woeid) {
		$meta = $this->get_meta();

		if (NULL === $meta) {
			$meta = array(
				'max_woeid' => (int)$woeid
			);
			$this->collections[self::META_COLLECTION]->insert($meta);
		}

		else if ((int)$woeid > (int)$meta['max_woeid']) {
			$criteria = array('_id' => 1);
			$meta['max_woeid'] = (int)$woeid;
			$options = array('upsert' => true);
			$this->collections[self::META_COLLECTION]->update($criteria, $params, $options);
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
