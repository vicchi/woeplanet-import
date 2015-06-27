#!/usr/bin/env php
<?php

require_once '../lib/task-runner.php';
require_once '../lib/timer.php';
require_once '../lib/cache-utils.php';

class CacheHierarchy extends Woeplanet\TaskRunner {
    const RUN_TASK = 'run';
    const CHILDREN_TASK = 'children';
    const ANCESTORS_TASK = 'ancestors';
    const SIBLINGS_TASK = 'siblings';
    const DESCENDANTS_TASK = 'descendants';
    const BELONGSTOS_TASK = 'belongstos';
    const TEST_TASK = 'test';

    private $cache;

    public function __construct($verbose, $task=NULL) {
        parent::__construct($verbose);

        $this->cache = new \Woeplanet\CacheUtils('geoplanet_cache.sqlite3');

        if ($task !== NULL) {
            $task = strtolower($task);
        }
        $this->task = $task;

        $this->tasks = array(
            self::RUN_TASK,
            self::CHILDREN_TASK,
            self::ANCESTORS_TASK,
            self::SIBLINGS_TASK,
            self::DESCENDANTS_TASK,
            self::BELONGSTOS_TASK,
            self::TEST_TASK
        );

        $this->timer = new Woeplanet\Timer($this->tasks);

        $this->sqlite = array(
            'path' => 'geoplanet_cache.sqlite3',
            'handle' => NULL
        );
    }

    public function run() {
        $this->timer->elapsed(self::RUN_TASK);

        if (isset($this->task)) {
            $this->timer->elapsed($this->task);

            $func = "task_$this->task";
            // $this->init_cache();
            $this->$func();

            $elapsed = $this->timer->seconds_to_time($this->timer->elapsed($this->task));
            $this->log("Completed task $this->task in $elapsed");
        }

        else {
            foreach ($this->tasks as $task) {
                if ($task !== self::RUN_TASK && $task !== self::TEST_TASK) {
                    $this->timer->elapsed($task);

                    $func = "task_$task";
                    $this->task = $task;
                    // $this->init_cache();
                    $this->$func();

                    $elapsed = $this->timer->seconds_to_time($this->timer->elapsed($task));
                    $this->log("Completed stage $task in $elapsed");
                }
            }
        }

        $elapsed = $this->timer->seconds_to_time($this->timer->elapsed(self::RUN_TASK));
        $this->log("Completed in $elapsed");
    }

    private function task_children() {
        $db = $this->cache->get_cache();

        $this->log('Resetting children cache');
        $this->cache->create_table(\Woeplanet\CacheUtils::CHILDREN_TABLE, true);

        $this->log('Preparing queries');
        $sql = 'SELECT woeid FROM places WHERE parent = :parent;';
        $select = $db->prepare($sql);
        if (!$select) {
            error_log('cache_children/prepare select: ' . var_export($db->errorInfo(), true));
        }

        $sql = 'INSERT INTO children(woeid, children) VALUES(:woeid,:children);';
        $insert = $db->prepare($sql);
        if (!$insert) {
            error_log('cache_children/prepare insert: ' . var_export($db->errorInfo(), true));
        }

        $maxwoeid = $this->cache->get_maxwoeid();
        $this->log("Aggregating and caching children for $maxwoeid candidate woeids");

        $sql = 'SELECT woeid FROM places;';
        $find = $db->prepare($sql);
        $find->execute();

        while (($doc = $find->fetch(\PDO::FETCH_ASSOC)) !== false) {
            $woeid = intval($doc['woeid']);

            $children = array();
            $select->bindParam(':parent', $woeid);
            if (!$select->execute()) {
                error_log('cache_children/select: ' . var_export($db->errorInfo(), true));
            }

            else {
                while (($doc = $select->fetch(\PDO::FETCH_ASSOC)) !== false) {
                    $children[] = intval($doc['woeid']);
                }
                $values = array(
                    ':woeid' => intval($woeid),
                    ':children' => serialize($children)
                );
                if (!$insert->execute($values)) {
                    error_log('cache_children/insert: ' . var_export($db->errorInfo(), true));
                }
            }

            $this->show_status($woeid, $maxwoeid);
        }

        $this->cache->create_index(\Woeplanet\CacheUtils::CHILDREN_TABLE);
        $this->log("Finished aggregating and caching children for $maxwoeid candidate woeids");
    }

    private function task_ancestors() {
        $db = $this->cache->get_cache();

        $this->log('Resetting ancestors cache');
        $this->cache->create_table(\Woeplanet\CacheUtils::ANCESTORS_TABLE, true);

        // $db->exec('DROP TABLE IF EXISTS ancestors');
        // $this->define_table($this->stage);

        $maxwoeid = $this->cache->get_maxwoeid();

        $sql = 'INSERT INTO ancestors(woeid, ancestors) VALUES(:woeid,:ancestors);';
        $insert = $db->prepare($sql);

        $sql = 'SELECT woeid FROM places;';
        $select = $db->prepare($sql);
        $select->execute();

        $this->log("Aggregating and caching ancestors for $maxwoeid candidate woeids");

        while (($doc = $select->fetch(\PDO::FETCH_ASSOC)) !== false) {
            $woeid = $target = $doc['woeid'];
            $parents = array();

            while ($p = $this->cache->get_parent($target)) {
                if (in_array($p, $parents)) {
                    throw new Exception("Recursion trap: $p is an ancestor of itself!");
                }
                $parents[] = $p;
                $target = $p;
            }

            if (count($parents) == 0) {
                continue;
            }

            $insert->bindValue(':woeid', $woeid);
            $insert->bindValue(':ancestors', serialize($parents));

            if (!$insert->execute()) {
                error_log('cache_ancestors/insert: ' . var_export($db->errorInfo(), true));
            }

            $this->show_status($woeid, $maxwoeid);
        }

        $this->cache->create_index(\Woeplanet\CacheUtils::ANCESTORS_TABLE);
        $this->log("Finished aggregating and caching ancestors for $maxwoeid candidate woeids");
    }

    private function task_siblings() {
        $db = $this->cache->get_cache();

        $this->log('Resetting siblings cache');
        $this->cache->create_table(\Woeplanet\CacheUtils::SIBLINGS_TABLE, true);

        $maxwoeid = $this->cache->get_maxwoeid();

        $sql = 'INSERT INTO siblings(woeid, siblings) VALUES(:woeid,:siblings);';
        $insert = $db->prepare($sql);

        $sql = 'SELECT woeid,parent,placetype FROM places;';
        $select = $db->prepare($sql);
        $select->execute();

        $row = 0;
        $this->log("Calculating and caching siblings for $maxwoeid candidate woeids");
        while (($doc = $select->fetch(\PDO::FETCH_ASSOC)) !== false) {
            $row++;
            $woeid = $doc['woeid'];
            $parent = $doc['parent'];
            if (!isset($doc['parent']) || empty($doc['parent'])) {
                continue;   // No parent? Unlikely
            }

            $children = $this->cache->get_children($parent);
            if ($children === NULL || empty($children)) {
                continue;
            }
            // PDO can't handle binding array values to query parameters, so for
            //
            // "SELECT woeid FROM places WHERE woeid IN (:children) AND placetype = :placetype"
            //
            // PDO should give me this: SELECT woeid FROM places WHERE woeid IN (1,2,3,4,5) AND placetype = 8
            // But is actually gives me this: SELECT woeid FROM places WHERE woeid IN ('1,2,3,4,5') AND placetype = 8
            // Which is pants.

            $children = implode(',', $children);
            $placetype = intval($doc['placetype']);

            $sql = 'SELECT woeid FROM places WHERE woeid IN (' . $children . ') AND placetype = ' . $placetype . ';';
            $search = $db->prepare($sql);
            if (!$search) {
                error_log('cache_siblings/search: ' . var_export($db->errorInfo(), true));
                continue;
            }

            if (!$search->execute()) {
                error_log('cache_siblings/search: ' . var_export($db->errorInfo(), true));
                continue;
            }

            $siblings = array();
            while(($sibling = $search->fetch(PDO::FETCH_ASSOC)) !== FALSE) {
                $siblings[] = intval($sibling['woeid']);
            }

            $values = array(
                ':woeid' => $doc['woeid'],
                ':siblings' => serialize($siblings)
            );
            if (!$insert->execute($values)) {
                error_log('cache_siblings/insert: ' . var_export($db->errorInfo(), true));
                continue;
            }

            $this->show_status($row, $maxwoeid);
        }

        $this->cache->create_index(\Woeplanet\CacheUtils::SIBLINGS_TABLE);
        $this->log("Finished calculating and caching siblings for $maxwoeid candidate woeids");
    }

    private function task_descendants() {
        $db = $this->cache->get_cache();

        $this->log('Resetting descendants cache');
        $this->cache->create_table(\Woeplanet\CacheUtils::DESCENDANTS_TABLE, true);

        $maxwoeid = $this->cache->get_maxwoeid();

        $sql = 'SELECT woeid FROM places;';
        $select = $db->prepare($sql);

        $this->log("Calculating and caching descendants for $maxwoeid candidate woeids");
        $row = 0;

        $select->execute();
        // error_log("Recursing into find_descendants");
        while (($doc = $select->fetch(PDO::FETCH_ASSOC)) !== false) {
            $row++;
            $woeid = intval($doc['woeid']);
            if ($woeid === 1) {
                continue;
            }
            $this->find_descendants(intval($doc['woeid']));
            $this->show_status($row, $maxwoeid);
        }

        $this->cache->create_index(\Woeplanet\CacheUtils::DESCENDANTS_TABLE);
        $this->log("Finished calculating and caching descendants for $maxwoeid candidate woeids");
    }

    private function task_belongstos() {
    }

    private function task_test() {
    }

    private function find_descendants($woeid) {
        // error_log("find descendants for $woeid");
        $db = $this->cache->get_cache();
        $comma = ',';
        $children = $this->cache->get_children($woeid);
        error_log($woeid . ': ' . count($children) . ' children');
        if (empty($children)) {
            return array();
        }

        $temp = sys_get_temp_dir() . '/geoplanet-descendants-' . $woeid . '.tmp';
        if (!$file = fopen($temp, 'w')) {
            error_log("Cannot open temp file $temp");
            exit;
        }

        error_log('Iterating through ' . count($children) . ' children of ' . $woeid);
        foreach ($children as $child) {
            $descendants = $this->find_descendants($child);
            if (!empty($descendants)) {
                $descendants[] = $child;
                fwrite($file, $comma . implode($comma, $descendants));
            }
            else {
                $descendants = array($child);
                fwrite($file, $comma . $child);
            }
            unset($descendants);
        }

        fclose($file);
        unset($file);

        $descendants = file_get_contents($temp);
        if ($descendants === false) {
            error_log("Cannot read temp file $temp");
            exit;
        }
        $descendants = explode($comma, $descendants);
        $descendants = array_filter($descendants);
        $descendants = array_unique($descendants);

        $sql = 'INSERT OR REPLACE INTO descendants(woeid, descendants) VALUES(:woeid, :descendants);';
        $insert = $db->prepare($sql);
        if (!$insert) {
            error_log('find_descendants: prepare: ' . var_export($db->errorInfo(), true));
            exit;
        }
        $values = array(':woeid' => $woeid, ':descendants' => serialize($descendants));
        if (!$insert->execute($values)) {
            if (!$insert) {
                error_log('find_descendants: insert: ' . var_export($db->errorInfo(), true));
                exit;
            }
        }
        return $descendants;
    }
}

$shortopts = "vt:";
$longopts = array(
    "verbose",
    "task:"
);

$verbose = false;
$task = NULL;

$options = getopt($shortopts, $longopts);

if (isset($options['v']) || isset($options['verbose'])) {
    $verbose = true;
}
if (isset($options['t'])) {
    $task = $options['t'];
}
else if (isset($options['task'])) {
    $task = $options['task'];
}

$cache = new CacheHierarchy($verbose, $task);
$cache->run();

?>
