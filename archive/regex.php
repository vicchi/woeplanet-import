#!/usr/bin/env php
<?php

$path = '~/Data/GeoPlanet/geoplanet_data_7.10.0';
$pattern = '/(.+)_([\d\.]+)$/';
$ret = preg_match($pattern, basename($path), $match);
var_dump($ret);
if ($ret === 1) {
	var_dump($match);
}

?>
