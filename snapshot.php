<?php

require 'vendor/autoload.php';

$params = array(
    'hosts' => array('http://localhost:9200')
);
$es = new Elasticsearch\Client($params);

$params = array(
    'index' => 'woeplanet'
);
$es->indices()->close($params);

$params = array(
    //'indices' => array('woeplanet'),
    'repository' => 'woeplanet',
    'snapshot' => 'geoplanet_7.4.1'
);
$es->snapshot()->create($params);


?>
