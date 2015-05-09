<?php

namespace Woeplanet;

class Timer {
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
        $dtf = new \DateTime("@0");
        $dtt = new \DateTime("@$seconds");
        return $dtf->diff($dtt)->format('%h hours, %i mins, %s secs');
    }
}

?>
