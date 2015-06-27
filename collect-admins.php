#!/usr/bin/env php
<?php

require_once 'runner.php';
require_once 'timer.php';
require_once 'vendor/autoload.php';

class AdminsCollector extends Woeplanet\Runner {
    const RUN_STAGE = 'run';
    const SETUP_STAGE = 'setup';
    const ADMINS_STAGE = 'admins';
    const TEST_STAGE = 'test';

    private $path;

    private function index_admins() {
        $meta = $this->get_meta();
        $total = (int)$meta['_source']['max_woeid'];
        $woeid = 1;

        $this->log("Collecting admin hierarchy for $total candidate WOEIDs");
        while ($woeid <= $total) {
            $woeid++;

            $doc = $this->get_woeid($woeid);
            if ($doc['found']) {
                $admins = array(
                    'woe:id' => (int)$woeid,
                    'woe:state' => 0,
                    'woe:county' => 0,
                    'woe:local-admin' => 0,
                    'woe:country' => 0,
                    'woe:continent' => 0
                );
                $admins = $this->collect_admins($woeid, $admins);
                $params = array(
                    'body' => $admins,
                    'index' => self::INDEX,
                    'type' => self::ADMINS_TYPE,
                    'id' => (int)$woeid
                );
                $params['body']['history'] = $this->history;
                $this->es->index($params);
            }
            $this->show_status($woeid, $total);
        }

        $this->log("\nFinished indexing $woeid of $total candidate WOEIDs");
    }

}
?>
