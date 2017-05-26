<?php

namespace Woeplanet\Utils;

class Timer {
    static $last;

    public function __construct($tasks) {
        date_default_timezone_set('UTC');
        self::$last = [];
        foreach ($tasks as $task) {
            self::$last[$task] = null;
        }
    }

    public function elapsed($task) {
        $now = time();
        $elapsed = $now;
        if (self::$last[$task] != null) {
            $elapsed = ($now - self::$last[$task]);
        }

        self::$last[$task] = $now;
        return $elapsed;
    }

    public function seconds_to_time($seconds) {
        $dtf = new \DateTime("@0");
        $dtt = new \DateTime("@$seconds");
        return $dtf->diff($dtt)->format('%h hours, %i mins, %s secs');
    }
}

?>
