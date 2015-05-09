<?php

namespace Woeplanet;

class DataFile {
	private $handle = NULL;
	private $line_length = 1000;
	private $separator = "\t";
	private $header = NULL;
	private $path;

	public function __construct($path) {
        $this->path = $path;
	}

	public function open($path) {
		$this->path = $path;
		if ($this->handle !== NULL) {
			$this->close();
		}
		if (($this->handle = fopen($this->path, "r")) === false) {
			throw new Exception('Failed to open ' . $path);
		}

		$this->header = fgetcsv($this->handle, $this->line_length, $this->separator);
	}

	public function get() {
        if ($this->handle === NULL) {
            $this->open($this->path);
        }
		if (($data = fgetcsv($this->handle, $this->line_length, $this->separator)) !== false) {
			$row = array();
			foreach ($this->header as $i => $column) {
				$row[$column] = $data[$i];
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
