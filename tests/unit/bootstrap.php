<?php

if (!function_exists('curl_init')) {
    die("Cannot run tests; cURL must be enabled!");
}

if (!($loader = include __DIR__ . '/../../vendor/autoload.php')) {
    die("Cannot run tests; install dependencies via Composer!");
}

$loader->add('Woeplanet\Tests', __DIR__);
