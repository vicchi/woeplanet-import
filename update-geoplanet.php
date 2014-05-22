#!/usr/bin/env php -q
<?php

require 'vendor/autoload.php';
require 'geoplanet-data-reader.php';
require 'oauth.php';

use GuzzleHttp\Client;
use GuzzleHttp\Subscriber\Oauth\Oauth1;

$shortopts = "vg:s:";
$longopts = array(
	"verbose",
	"geoplanet:",
	"stage:"
);

$verbose = false;
$purge = false;
$path = NULL;
$stage = NULL;

$options = getopt($shortopts, $longopts);

if (isset($options['v']) || isset($options['verbose'])) {
	$verbose = true;
}

if (isset($options['g'])) {
	$path = $options['g'];
}
else if (isset($options['geoplanet'])) {
	$path = $options['geoplanet'];
}
else {
	echo "Missing path to GeoPlanet Data\n";
	exit;
}

if (isset($options['s'])) {
	$stage = $options['s'];
}
else if (isset($options['stage'])) {
	$stage = $options['stage'];
}

$updater = new GeoPlanetUpdater($path, $verbose, $stage, $key, $secret);
$updater->run();

class GeoPlanetUpdater {
	const CACHE_STAGE = 'cache';
	const UPDATE_STAGE = 'update';

	const PLACES_WOEID = 'WOE_ID';
	const PLACES_ISO = 'ISO';
	const PLACES_NAME = 'Name';
	const PLACES_LANGUAGE = 'Language';
	const PLACES_PLACETYPE = 'PlaceType';
	const PLACES_PARENTID = 'Parent_ID';

	private $seconds;
	private $day;
	private $rate;
	private $started;

	private $verbose = false;
	private $path = NULL;
	private $stage = NULL;

	private $version;
	private $provider;

	private $places;
	private $files;

	private $key;
	private $secret;

	public function __construct($path, $verbose, $stage, $key, $secret) {
		$this->path = $path;
		$this->verbose = $verbose;
		$stage = strtolower($stage);
		if ($stage == NULL) {
			$stage = 'run';
		}
		$this->stage = $stage;
		$this->key = $key;
		$this->secret = $secret;

		$this->seconds = 1000000;
		$this->day = $this->seconds * 60 * 60 * 24;
		$this->rate = $this->day / 50000;
		$this->started = microtime(true);

		$match = array();
		$pattern = '/geoplanet_data_([\d\.]+)$/';
		$ret = preg_match($pattern, basename($this->path), $match);
		$this->version = $match[1];
		$this->provider = sprintf('geoplanet %s', $this->version);

		$this->places = sprintf('geoplanet_places_%s.tsv', $this->version);

		$this->files = array();

		$dir = opendir($this->path);
		while (false !== ($entry = readdir($dir))) {
			if ($entry === '.' || $entry === '..')
				continue;

			$this->files[$entry] = $this->path . DIRECTORY_SEPARATOR . $entry;
		}

		if (false === array_key_exists($this->places, $this->files)) {
			$this->log("Missing $places");
			exit;
		}
	}

	public function run() {
		$this->elapsed('run');

		if ($this->stage == 'run' || $this->stage == 'cache') {
			$this->cache_places();
			if ($this->stage != 'run')
				exit;
		}

		if ($this->stage == 'run' || $this->stage == 'update') {
			$this->update_places();
			if ($this->stage != 'run')
				exit;
		}

		$elapsed = $this->seconds_to_time($this->elapsed('run'));
		$this->log("Completed in $elapsed");
	}

	private function cache_places() {
		$this->elapsed('cache');
		$db = new PDO('sqlite:geoplanet_updates.sqlite3');
		$tsv = new GeoPlanetDataReader();

		$tsv->open($this->files[$this->places]);
		$total = $tsv->size();

		$this->log("Caching $total WOEIDs from YQL");

		$setup = "CREATE TABLE geoplanet_update(
			woeid INTEGER PRIMARY KEY,
			parent INTEGER,
			timezone INTEGER,
			tz STRING
		);";
		$db->exec($setup);

		$insert = "INSERT INTO geoplanet_update(woeid,parent,timezone,tz) VALUES(:woeid,:parent,:timezone,:tz)";
		$statement = $db->prepare($insert);
		$row = 0;

		$client = new GuzzleHttp\Client(['
			base_url' => 'https://query.yahooapis.com/v1/public/yql/',
			'defaults' => ['auth'  => 'oauth']
		]);
		$oauth = new GuzzleHttp\Subscriber\Oauth\Oauth1([
			'consumer_key' => $this->key,
			'consumer_secret' => $this->secret
		]);
		$client->getEmitter()->attach($oauth);

		$q = 'https://query.yahooapis.com/v1/public/yql/?q=';
		$select = 'select woeid,timezone from geo.places.parent where child_woeid = %d';
		$format = '&format=json';

		$query = 'https://query.yahooapis.com/v1/public/yql' .
			$q .
			urlencode($select) .
			$format;

		while (($data = $tsv->get()) !== false) {
			$row++;

			$this->delay();

			$yql = $q .
				urlencode(sprintf($select, $data[self::PLACES_WOEID])) .
				$format;
			$res = $client->get($yql);

			if ($res->getStatusCode() == '200') {
				$statement->bindParam(':woeid', $data['WOE_ID']);
				$json = $res->json();

				$statement->bindParam(':parent', $json['query']['results']['place']['woeid']);
				$timezone = 0;
				$tz = '';
				if (isset($json['query']['results']['place']['timezone'])) {
					$timezone = $json['query']['results']['place']['timezone']['woeid'];
					$tz = $json['query']['results']['place']['timezone']['content'];
				}
				$statement->bindParam(':timezone', $timezone);
				$statement->bindParam(':tz', $tz);
				$statement->execute();
			}

			$this->show_status($data['WOE_ID'], $row, $total);
		}

		$elapsed = $this->seconds_to_time($this->elapsed('cache'));
		$this->log("\nCompleted GeoPlanet update caching in $elapsed");

	}

	private function update_places() {

	}

	private function delay() {
		$ended = microtime(true);
		$elapsed = ($ended - $this->started) * $this->seconds;

		if ($elapsed < $this->rate) {
			usleep($this->rate - $elapsed);
		}

		$this->started = microtime(true);
	}

	private function elapsed($stage) {
		static $last = array(
			'run' => NULL,
			'cache' => NULL,
			'update' => NULL
		);

		$now = time();
		$elapsed = $now;
		if ($last[$stage] !== NULL) {
			$elapsed = ($now - $last[$stage]);
		}
		$last[$stage] = $now;
		return $elapsed;
	}

	private function seconds_to_time($seconds) {
		$dtf = new DateTime("@0");
		$dtt = new DateTime("@$seconds");
		return $dtf->diff($dtt)->format('%h hours, %i mins, %s secs');
	}

	private function log($message) {
		echo "$message\n";
	}

	private function logVerbose($message) {
		if ($this->verbose) {
			$this->log($message);
		}
	}

	// Thanks to Brian Moon for this - http://brian.moonspot.net/php-progress-bar
	private function show_status($woeid, $done, $total, $size=30) {
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
		$status_bar .= "] $disp%  $done/$total woeid:$woeid";
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
