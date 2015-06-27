<?php

require 'vendor/autoload.php';

$es = new Elasticsearch\Client();

$params = array(
	'index' => 'test',
	'ignore' => 404
);
$ret = $es->indices()->delete($params);
var_dump($ret);

$params = array(
	'body' => array(
		'foo' => 'foo',
		'bar' => 'bar'
	),
	'index' => 'test',
	'type' => 'testtype',
	'id' => 1
);
$ret = $es->index($params);
var_dump($ret);

$id = 1;
$params = array(
	'index' => 'test',
	'type' => 'testtype',
	'id' => 1,
	'ignore' => 404
);
echo "Query for id '1'\n";
$ret = $es->get($params);
echo "Return type: " . gettype($ret) . "\n";
var_dump($ret);

$params['id'] = 2;
echo "Query for id '2'\n";
$ret = $es->get($params);
echo "Return type: " . gettype($ret) . "\n";
var_dump($ret);
