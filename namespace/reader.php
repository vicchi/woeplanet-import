<?php

namespace Woeplanet;

class Reader {
	private $handle = NULL;
	private $line_length = 1000;
	private $separator = "\t";
	private $header = NULL;
	private $path;

	public function __construct() {
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
