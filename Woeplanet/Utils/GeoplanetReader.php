<?php

namespace Woeplanet\Utils;

class GeoplanetReader {
    private $defaults = [
        'encoding' => 'UTF-8',
        'line-length' => 1000,
        'separator' => "\t"
    ];
    private $settings = [];
    private $path = '';
    private $handle = null;
    private $header = [];

    public function __construct() {
    }

    public function open($path, $settings=[]) {
        $this->settings = array_merge($this->defaults, $settings);
        $this->path = $path;

        if ($this->handle !== NULL) {
            $this->close();
        }

        if (($this->handle = fopen($this->path, "r")) === false) {
            throw new \Exception('Failed to open ' . $path);
        }

        $this->header = fgetcsv($this->handle, $this->settings['line-length'], $this->settings['separator']);
        if (count($this->header) == 1) {
            // Try and de-bork the suspect (and space separated) headers that sometimes
            // crop up (v7.4.0 changes file, I'm looking at you here)
            $header = implode("", $this->header);
            $header = preg_replace("/[[:blank:]]+/", " ", $header);
            $header = preg_replace('/"/', "", $header);
            $this->header = explode(" ", $header);
        }
    }

    public function get() {
        if (($data = fgetcsv($this->handle, $this->settings['line-length'], $this->settings['separator'])) !== false) {
            $row = [];
            foreach ($this->header as $i => $key) {
                $value = $data[$i];
                if (!mb_check_encoding($value, $this->settings['encoding'])) {
                    $value = utf8_encode($value);
                }
                $row[$key] = $value;
            }
            return $row;
        }

        return false;
    }

    public function size() {
        $handle = fopen($this->path, "r");
        $count = 0;
        while (fgets($handle)) {
            $count++;
        }
        fclose($handle);
        return --$count;
    }

    public function close() {
        fclose($this->handle);
        $this->handle = NULL;
    }

}

?>
