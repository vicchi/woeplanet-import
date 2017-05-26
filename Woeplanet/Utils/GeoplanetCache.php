<?php

namespace Woeplanet\Utils;

class GeoplanetCache {
    private $path;
    private $handle;
    private $setup;

    const META_TABLE = 'meta';
    const PLACES_TABLE = 'places';
    const ADJACENCIES_TABLE = 'adjacencies';
    const ALIASES_TABLE = 'aliases';
    const PLACETYPES_TABLE = 'placetypes';
    const ADMINS_TABLE = 'admins';
    const CHILDREN_TABLE = 'children';
    const ANCESTORS_TABLE = 'ancestors';
    const SIBLINGS_TABLE = 'siblings';
    const DESCENDANTS_TABLE = 'descendants';
    const BELONGSTOS_TABLE = 'belongstos';
    const COORDS_TABLE = 'coords';
    const CONCORDANCE_TABLE = 'concordance';
    const COUNTRIES_TABLE = 'countries';

    const GET_META = 'get_meta';
    const GET_WOEID = 'get_woeid';
    const GET_PARENT = 'get_parent';
    const GET_CHILDREN = 'get_children';
    const GET_ADMINS = 'get_admins';
    const GET_COORDS = 'get_coords';
    const GET_ANCESTORS = 'get_ancestors';

    private $fields;
    private $sql;
    private $statements;
    private $schema_change;

    public function __construct($path, $setup=true) {
        $this->path = $path;
        $this->db = NULL;
        $this->setup = $setup;
        $this->schema_change = false;

        $this->init_cache();

        $this->fields = [
            'places' => [
                'woeid' => \PDO::PARAM_INT,
                'iso' => \PDO::PARAM_STR,
                'name' => \PDO::PARAM_STR,
                'lang' => \PDO::PARAM_STR,
                'placetype' => \PDO::PARAM_INT,
                'placetypename' => \PDO::PARAM_STR,
                'parent' => \PDO::PARAM_INT,
                'lon'  => \PDO::PARAM_STR,
                'lat' => \PDO::PARAM_STR,
                'swlon' => \PDO::PARAM_STR,
                'swlat' => \PDO::PARAM_STR,
                'nelon' => \PDO::PARAM_STR,
                'nelat' => \PDO::PARAM_STR,
                'adjacent' => \PDO::PARAM_STR,
                'alias_q' => \PDO::PARAM_STR,
                'alias_v' => \PDO::PARAM_STR,
                'alias_a' => \PDO::PARAM_STR,
                'alias_s' => \PDO::PARAM_STR,
                'alias_p' => \PDO::PARAM_STR,
                'state' => \PDO::PARAM_INT,
                'county' => \PDO::PARAM_INT,
                'localadmin' => \PDO::PARAM_INT,
                'country' => \PDO::PARAM_INT,
                'continent' => \PDO::PARAM_INT,
                'supercedes' => \PDO::PARAM_STR,
                'superceded' => \PDO::PARAM_INT,
                'history' => \PDO::PARAM_STR
            ],
            'adjacencies' => [
                'woeid' => \PDO::PARAM_INT,
                'neighbour' => \PDO::PARAM_STR
            ],
            'aliases' => [
                'woeid' => \PDO::PARAM_INT,
                'name' => \PDO::PARAM_STR,
                'type' => \PDO::PARAM_STR,
                'lang' => \PDO::PARAM_STR
            ],
            'placetypes' => [
                'id' => \PDO::PARAM_INT,
                'name' => \PDO::PARAM_STR,
                'descr' => \PDO::PARAM_STR,
                'shortname' => \PDO::PARAM_STR,
                'tag' => \PDO::PARAM_STR
            ],
            'admins' => [
                'woeid' => \PDO::PARAM_INT,
                'state' => \PDO::PARAM_INT,
                'county' => \PDO::PARAM_INT,
                'localadmin' => \PDO::PARAM_INT,
                'country' => \PDO::PARAM_INT,
                'continent' => \PDO::PARAM_INT
            ],
            'countries' => [
                'woeid' => \PDO::PARAM_INT,
                'name' => \PDO::PARAM_STR,
                'iso2' => \PDO::PARAM_STR,
                'iso3' => \PDO::PARAM_STR
            ]
        ];

        $this->sql = [
            self::GET_META => 'SELECT DISTINCT * FROM meta WHERE id = 1;',
            self::GET_WOEID => 'SELECT DISTINCT * FROM places WHERE woeid = :woeid;',
            self::GET_PARENT => 'SELECT parent FROM places WHERE woeid = :woeid;',
            self::GET_CHILDREN => 'SELECT children from children WHERE woeid = :woeid;',
            self::GET_ADMINS => 'SELECT * FROM admins WHERE woeid= :woeid;',
            self::GET_COORDS => 'SELECT * FROM coords WHERE woeid= :woeid;',
            self::GET_ANCESTORS => 'SELECT ancestors FROM ancestors WHERE woeid= :woeid;'
        ];

        $this->statements = [
            self::GET_META => NULL,
            self::GET_WOEID => NULL,
            self::GET_PARENT => NULL,
            self::GET_CHILDREN => NULL,
            self::GET_ADMINS => NULL,
            self::GET_COORDS => NULL,
            self::GET_ANCESTORS => NULL
        ];

        // if ($this->setup) {
        //     foreach ($this->sql as $key => $sql) {
        //         if (!($this->statements[$key] = $this->db->prepare($sql))) {
        //             throw new \Exception($sql . ':' . $this->get_cache_error());
        //         }
        //     }
        // }
    }

    public function init_cache() {
        if ($this->db === NULL) {
            $name = 'sqlite:' . $this->path;
            $this->db = new \PDO($name);
        }

        $this->db->exec('PRAGMA synchronous=0');
        $this->db->exec('PRAGMA locking_mode=EXCLUSIVE');
        $this->db->exec('PRAGMA journal_mode=DELETE');
        // $this->db->exec('PRAGMA journal_mode=WAL');
        $this->db->exec('PRAGMA main.page_size=4096');
        $this->db->exec('PRAGMA main.cache_size=10000');
    }

    private function prepare($function) {
        if ($this->schema_change) {
            $this->schema_change = false;
            foreach ($this->statements as $key => $statement) {
                $this->statements[$key] = NULL;
            }
        }

        if (NULL === $this->statements[$function]) {
            $sql = $this->sql[$function];
            if (!($this->statements[$function] = $this->db->prepare($sql))) {
                throw new \Exception($sql . ':' . $this->get_cache_error());
            }
        }
        return $this->statements[$function];
    }

    public function get_cache() {
        return $this->db;
    }

    public function get_meta() {
        $query = $this->prepare(__FUNCTION__);
        if (!$query->execute()) {
            throw new \Exception(sprintf('%s: PDOStatement::execute() (%s)', __FUNCTION__, $this->get_cache_error()));
        }
        // if (!($return = $query->fetch(\PDO::FETCH_ASSOC))) {
        //     throw new \Exception(sprintf('%s: PDOStatement::fetch() (%s)', __FUNCTION__, $this->get_cache_error()));
        // }

        $return = $query->fetch(\PDO::FETCH_ASSOC);
        if (empty($return)) {
            $return = NULL;
        }

        $query->closeCursor();
        return $return;
        // $sql = 'SELECT DISTINCT * FROM meta WHERE id = 1;';
        // return $this->db->query($sql)->fetch(\PDO::FETCH_ASSOC);
    }

    public function get_woeid($woeid) {
        $query = $this->prepare(__FUNCTION__);
        $query->bindValue(':woeid', $woeid, $this->fields['places']['woeid']);
        if (!$query->execute()) {
            throw new \Exception(sprintf('%s: PDOStatement::execute() (%s)', __FUNCTION__, $this->get_cache_error()));
        }

        // if (!($return = $query->fetch(\PDO::FETCH_ASSOC))) {
        //     throw new \Exception(sprintf('%s: PDOStatement::fetch() (%s)', __FUNCTION__, $this->get_cache_error()));
        // }

        $return = $query->fetch(\PDO::FETCH_ASSOC);
        if (!empty($return)) {
            $return = $this->unpack_place($return);
        }
        else {
            $return = NULL;
        }

        $query->closeCursor();
        return $return;
        //
        // $sql = 'SELECT DISTINCT * FROM places WHERE woeid = :woeid;';
        // $select = $this->db->prepare($sql);
        // $select->bindParam(':woeid', $woeid, $this->fields['places']['woeid']);
        // $select->execute();
        // $ret = $select->fetch(\PDO::FETCH_ASSOC);
        // if ($ret) {
        //     return $this->unpack_place($ret);
        // }
        // return NULL;
    }

    public function get_maxwoeid() {
        $meta = $this->get_meta();
        if (isset($meta['maxwoeid']) && !empty($meta['maxwoeid'])) {
            return intval($meta['maxwoeid']);
        }

        return 0;
    }

    public function get_parent($woeid) {
        $query = $this->prepare(__FUNCTION__);
        $query->bindValue(':woeid', intval($woeid), $this->fields['places']['woeid']);
        if (!$query->execute()) {
            throw new \Exception(sprintf('%s: PDOStatement::execute() (%s)', __FUNCTION__, $this->get_cache_error()));
        }
        $return = $query->fetchAll(\PDO::FETCH_ASSOC);
        if ($return === false) {
            throw new \Exception(sprintf('%s: PDOStatement::fetchAll() (%s)', __FUNCTION__, $this->get_cache_error()));
        }

        if (!empty($return)) {
            $return = $return[0]['parent'];
        }
        else {
            $return = NULL;
        }

        $query->closeCursor();
        return $return;

        // $sql = 'SELECT parent FROM places WHERE woeid = :woeid;';
        // $select = $this->db->prepare($sql);
        // $select->execute([':woeid' => $woeid]);
        // $ret = $select->fetch(\PDO::FETCH_ASSOC);
        // if ($ret) {
        //     return $ret['parent'];
        // }
        // return NULL;
    }

    public function get_children($woeid) {
        $query = $this->prepare(__FUNCTION__);
        $query->bindValue(':woeid', intval($woeid));
        if (!$query->execute()) {
            throw new \Exception(sprintf('%s: PDOStatement::execute() (%s)', __FUNCTION__, $this->get_cache_error()));
        }
        $return = $query->fetchAll(\PDO::FETCH_ASSOC);
        if ($return === false) {
            throw new \Exception(sprintf('%s: PDOStatement::fetchAll() (%s)', __FUNCTION__, $this->get_cache_error()));
        }

        if (!empty($return)) {
            $return = unserialize($return[0]['children']);
        }
        else {
            $return = NULL;
        }

        $query->closeCursor();
        return $return;

        // $sql = 'SELECT children from children WHERE woeid = :woeid;';
        // $select = $this->db->prepare($sql);
        // $select->execute([':woeid' => $woeid]);
        // $ret = $select->fetch(\PDO::FETCH_ASSOC);
        // if ($ret) {
        //     return unserialize($ret['children']);
        // }
        // return NULL;
    }

    public function get_admins($woeid) {
        $query = $this->prepare(__FUNCTION__);
        $query->bindValue(':woeid', intval($woeid));
        if (!$query->execute()) {
            throw new \Exception(sprintf('%s: PDOStatement::execute() (%s)', __FUNCTION__, $this->get_cache_error()));
        }
        $return = $query->fetchAll(\PDO::FETCH_ASSOC);
        if ($return === false) {
            throw new \Exception(sprintf('%s: PDOStatement::fetchAll() (%s)', __FUNCTION__, $this->get_cache_error()));
        }

        if (!empty($return)) {
            $return  = $return[0];
        }
        else {
            $return = NULL;
        }

        $query->closeCursor();
        return $return;

        // $sql = 'SELECT * FROM admins WHERE woeid=' . intval($woeid) . ';';
        // if (!($query = $this->db->prepare($sql))) {
        //     $err = $this->get_cache_error();
        //     error_log("get_admins for $woeid failed prepare: $err");
        //     return NULL;
        // }
        //
        // if (!($query->execute())) {
        //     $err = $this->get_cache_error();
        //     error_log("get_admins for $woeid failed execute: $err");
        //     return NULL;
        // }
        //
        // $value = $query->fetchAll(\PDO::FETCH_ASSOC);
        // if ($value === false) {
        //     $err = $this->get_cache_error();
        //     error_log("get_admins for $woeid failed fetchAll: $err");
        //     return NULL;
        // }
        //
        // else if (!empty($value)) {
        //     return $value[0];
        // }
        //
        // return NULL;
    }

    public function get_coords($woeid) {
        $query = $this->prepare(__FUNCTION__);
        $query->bindValue(':woeid', intval($woeid));
        if (!($query->execute())) {
            throw new \Exception(sprintf('%s: PDOStatement::execute() (%s)', __FUNCTION__, $this->get_cache_error()));
        }
        $return = $query->fetchAll(\PDO::FETCH_ASSOC);
        if ($return === false) {
            throw new \Exception(sprintf('%s: PDOStatement::fetchAll() (%s)', __FUNCTION__, $this->get_cache_error()));
        }

        if (!empty($return)) {
            $return = $return[0];
        }
        else {
            $return = NULL;
        }

        $query->closeCursor();
        return $return;

        // $query = $this->statements[__FUNCTION__];
        // $query->bindValue(':woeid', intval($woeid));
        //
        // if (!($query->execute())) {
        //     $err = $this->get_cache_error();
        //     error_log("get_coords for $woeid failed execute: $err");
        //     return NULL;
        // }
        //
        // $value = $query->fetchAll(\PDO::FETCH_ASSOC);
        // if ($value === false) {
        //     $err = $this->get_cache_error();
        //     error_log("get_coords for $woeid failed fetchAll: $err");
        //     return NULL;
        // }
        //
        // else if (!empty($value)) {
        //     return $value[0];
        // }
        //
        // return NULL;
    }

    public function get_cache_error() {
        return implode(',', $this->db->errorInfo());
    }

    public function get_ancestors($woeid) {
        $query = $this->prepare(__FUNCTION__);
        $query->bindValue(':woeid', $woeid);
        if (!$query->execute()) {
            throw new \Exception(sprintf('%s: PDOStatement::execute() (%s)', __FUNCTION__, $this->get_cache_error()));
        }
        if (!($return = $query->fetch(\PDO::FETCH_ASSOC))) {
            throw new \Exception(sprintf('%s: PDOStatement::fetch() (%s)', __FUNCTION__, $this->get_cache_error()));
        }

        if (!empty($return)) {
            $return = unserialize($ret['ancestors']);
        }
        else {
            $return = NULL;
        }

        $query->closeCursor();
        return $return;
    }

    public function refresh_meta($woeid) {
        $meta = $this->get_meta();
        if (!$meta) {
            error_log('refresh_meta: no previous value, setting to ' . $woeid);
            $sql = 'INSERT OR REPLACE INTO meta(id, maxwoeid) VALUES(1, ' . $woeid. ');';
            $this->db->exec($sql);
        }

        else if ((int)$woeid > (int)$meta['maxwoeid']) {
            error_log('refresh_meta: setting maxwoeid to ' . $woeid);
            $sql = 'UPDATE meta SET maxwoeid = ' . $woeid . ' WHERE id = 1;';
            $this->db->exec($sql);
        }
    }

    public function insert_place($doc) {
        $fields = [];
        $keys = [];
        $values = [];

        $doc = $this->pack_place($doc);

        foreach ($doc as $key => $value) {
            $fields[] = $key;
            $keys[] = ':' . $key;
            $values[':' . $key] = [
                'value' => $value,
                'type' => $this->fields['places'][$key]
            ];
        }

        $sql = 'INSERT OR REPLACE INTO places(' . implode(',', $fields) . ') VALUES(' . implode(',', $keys) . ');';
        // error_log('sql: ' . $sql);
        $statement = $this->db->prepare($sql);
        if (!$statement) {
            throw new \Exception(sprintf('%s: PDOStatement::prepare() (%s)', __FUNCTION__, $this->get_cache_error()));
        }

        foreach ($values as $param => $value) {
            $statement->bindValue($param, $value['value'], $value['type']);
        }

        if (!$statement->execute()) {
            throw new \Exception(sprintf('%s: PDOStatement::execute() (%s)', __FUNCTION__, $this->get_cache_error()));
        }

        $statement->closeCursor();
        return true;
    }

    public function update_place($doc) {
        $values = [];
        $doc = $this->pack_place($doc);
        $woeid = $doc['woeid'];
        unset($doc['woeid']);

        // error_log(var_export($doc, true));
        $values = [];
        foreach ($doc as $key => $value) {
            $values[] = $key . ' = :' . $key;
        }

        $sql = 'UPDATE places SET ' . implode(',', $values) . ' WHERE woeid = :woeid';
        $statement = $this->db->prepare($sql);
        if (!$statement) {
            throw new \Exception(sprintf('%s: PDOStatement::prepare() (%s)', __FUNCTION__, $this->get_cache_error()));
        }

        foreach ($doc as $key => $value) {
            $statement->bindValue(':' . $key, $value, $this->fields['places'][$key]);
        }
        $statement->bindValue(':woeid', $woeid, $this->fields['places']['woeid']);

        if (!$statement->execute()) {
            throw new \Exception(sprintf('%s: PDOStatement::execute() (%s)', __FUNCTION__, $this->get_cache_error()));
        }

        $statement->closeCursor();
        return true;
    }

    public function insert_alias($doc) {
        $fields = [];
        $keys = [];
        $values = [];

        foreach ($doc as $key => $value) {
            $fields[] = $key;
            $keys[] = ':' . $key;
            $values[':' . $key] = [
                'value' => $value,
                'type' => $this->fields['aliases'][$key]
            ];
        }
        $sql = 'INSERT OR REPLACE INTO aliases(' . implode(',', $fields) . ') VALUES(' . implode(',', $keys) . ');';
        // error_log('sql: ' . $sql);
        $statement = $this->db->prepare($sql);
        if (!$statement) {
            throw new \Exception(sprintf('%s: PDOStatement::prepare() (%s)', __FUNCTION__, $this->get_cache_error()));
        }

        foreach ($values as $param => $value) {
            $statement->bindValue($param, $value['value'], $value['type']);
        }

        if (!$statement->execute()) {
            throw new \Exception(sprintf('%s: PDOStatement::execute() (%s)', __FUNCTION__, $this->get_cache_error()));
        }

        $statement->closeCursor();
        return true;
    }

    public function insert_admin($doc) {
        $fields = [];
        $keys = [];
        $values = [];
        $woeid = $doc['woeid'];

        foreach ($doc as $key => $value) {
            $fields[] = $key;
            $keys[] = ':' . $key;
            $values[':' . $key] = [
                'value' => $value,
                'type' => $this->fields['admins'][$key]
            ];
        }
        $sql = 'INSERT OR REPLACE INTO admins(' . implode(',', $fields) . ') VALUES(' . implode(',', $keys) . ');';
        // error_log('sql: ' . $sql);
        $statement = $this->db->prepare($sql);
        if (!$statement) {
            throw new \Exception(sprintf('%s: PDOStatement::prepare() (%s)', __FUNCTION__, $this->get_cache_error()));
        }

        foreach ($values as $param => $value) {
            $statement->bindValue($param, $value['value'], $value['type']);
        }

        if (!$statement->execute()) {
            throw new \Exception(sprintf('%s: PDOStatement::execute() (%s)', __FUNCTION__, $this->get_cache_error()));
        }

        $statement->closeCursor();
        return true;
    }

    public function insert_country($doc) {
        $fields = [];
        $keys = [];
        $values = [];
        $woeid = $doc['woeid'];

        foreach ($doc as $key => $value) {
            $fields[] = $key;
            $keys[] = ':' . $key;
            $values[':' . $key] = [
                'value' => $value,
                'type' => $this->fields['countries'][$key]
            ];
        }
        $sql = 'INSERT OR REPLACE INTO countries(' . implode(',', $fields) . ') VALUES(' . implode(',', $keys) . ');';
        // error_log('sql: ' . $sql);
        $statement = $this->db->prepare($sql);
        if (!$statement) {
            throw new \Exception(sprintf('%s: PDOStatement::prepare() (%s)', __FUNCTION__, $this->get_cache_error()));
        }

        foreach ($values as $param => $value) {
            $statement->bindValue($param, $value['value'], $value['type']);
        }

        if (!$statement->execute()) {
            throw new \Exception(sprintf('%s: PDOStatement::execute() (%s)', __FUNCTION__, $this->get_cache_error()));
        }

        $statement->closeCursor();
        return true;
    }

    public function unpack_place($doc) {
        // error_log('pre-unpack: ' . var_export($doc, true));
        if (isset($doc['adjacent']) && !empty($doc['adjacent'])) {
            $doc['adjacent'] = unserialize($doc['adjacent']);
        }
        if (isset($doc['alias_q']) && !empty($doc['alias_q'])) {
            $doc['alias_q'] = unserialize($doc['alias_q']);
        }
        if (isset($doc['alias_v']) && !empty($doc['alias_v'])) {
            $doc['alias_v'] = unserialize($doc['alias_v']);
        }
        if (isset($doc['alias_a']) && !empty($doc['alias_a'])) {
            $doc['alias_a'] = unserialize($doc['alias_a']);
        }
        if (isset($doc['alias_s']) && !empty($doc['alias_s'])) {
            $doc['alias_s'] = unserialize($doc['alias_s']);
        }
        if (isset($doc['alias_p']) && !empty($doc['alias_p'])) {
            $doc['alias_p'] = unserialize($doc['alias_p']);
        }
        if (isset($doc['supercedes']) && !empty($doc['supercedes'])) {
            $doc['supercedes'] = unserialize($doc['supercedes']);
        }
        if (isset($doc['history']) && !empty($doc['history'])) {
            $doc['history'] = unserialize($doc['history']);
        }
        // error_log('post-unpack: ' . var_export($doc, true));

        return $doc;
    }

    private function pack_place($doc) {
        if (empty($doc)) {
            throw new Exception('pack_place: empty document');
        }

        if (isset($doc['adjacent']) && !empty($doc['adjacent'])) {
            $doc['adjacent'] = serialize($doc['adjacent']);
        }
        if (isset($doc['alias_q']) && !empty($doc['alias_q'])) {
            $doc['alias_q'] = serialize($doc['alias_q']);
        }
        if (isset($doc['alias_v']) && !empty($doc['alias_v'])) {
            $doc['alias_v'] = serialize($doc['alias_v']);
        }
        if (isset($doc['alias_a']) && !empty($doc['alias_a'])) {
            $doc['alias_a'] = serialize($doc['alias_a']);
        }
        if (isset($doc['alias_s']) && !empty($doc['alias_s'])) {
            $doc['alias_s'] = serialize($doc['alias_s']);
        }
        if (isset($doc['alias_p']) && !empty($doc['alias_p'])) {
            $doc['alias_p'] = serialize($doc['alias_p']);
        }
        if (isset($doc['supercedes']) && !empty($doc['supercedes'])) {
            $doc['supercedes'] = serialize($doc['supercedes']);
        }
        if (isset($doc['history']) && !empty($doc['history'])) {
            $doc['history'] = serialize($doc['history']);
        }

        return $doc;
    }


    public function create_table($table, $reset=false) {
        $sql = '';
        switch ($table) {
            case self::META_TABLE:
                if ($reset) {
                    $sql .= 'DROP TABLE IF EXISTS meta;';
                }
                $sql .= 'CREATE TABLE IF NOT EXISTS meta (id INTEGER, maxwoeid INTEGER)';
                break;

            case self::PLACES_TABLE:
                if ($reset) {
                    $sql .= 'DROP TABLE IF EXISTS places;';
                }
                $sql .= 'CREATE TABLE IF NOT EXISTS places(
                    woeid INTEGER,
                    iso TEXT,
                    name TEXT,
                    lang TEXT,
                    placetype INTEGER,
                    placetypename TEXT,
                    parent INTEGER,
                    lon REAL,
                    lat REAL,
                    swlon REAL,
                    swlat REAL,
                    nelon REAL,
                    nelat REAL,
                    adjacent STRING,
                    alias_q STRING,
                    alias_v STRING,
                    alias_a STRING,
                    alias_s STRING,
                    alias_p STRING,
                    state INTEGER,
                    county INTEGER,
                    localadmin INTEGER,
                    country INTEGER,
                    continent INTEGER,
                    supercedes STRING,
                    superceded INTEGER,
                    history STRING
                );';
                break;

            case self::ADJACENCIES_TABLE:
                if ($reset) {
                    $sql .= 'DROP TABLE IF EXISTS adjacencies;';
                }
                $sql .= 'CREATE TABLE IF NOT EXISTS adjacencies(woeid INTEGER, neighbour INTEGER)';
                break;

            case self::ALIASES_TABLE:
                if ($reset) {
                    $sql .= 'DROP TABLE IF EXISTS aliases;';
                }
                $sql .= 'CREATE TABLE IF NOT EXISTS aliases(
                    woeid INTEGER,
                    name TEXT,
                    type TEXT,
                    lang TEXT
                );';
                break;

            case self::PLACETYPES_TABLE:
                if ($reset) {
                    $sql .= 'DROP TABLE IF EXISTS placetypes;';
                }
                $sql .= 'CREATE TABLE IF NOT EXISTS placetypes(id INTEGER, name STRING, descr STRING, shortname STRING, tag STRING);';
                break;

            case self::ADMINS_TABLE:
                if ($reset) {
                    $sql .= 'DROP TABLE IF EXISTS admins;';
                }
                $sql .= 'CREATE TABLE IF NOT EXISTS admins(woeid INTEGER, state INTEGER, county INTEGER, localadmin INTEGER, country INTEGER, continent INTEGER);';
                break;

            case self::CHILDREN_TABLE:
                if ($reset) {
                    $sql .= 'DROP TABLE IF EXISTS children;';
                }
                $sql .= 'CREATE TABLE IF NOT EXISTS children(woeid INTEGER, children TEXT);';
                break;

            case self::ANCESTORS_TABLE:
                if ($reset) {
                    $sql .= 'DROP TABLE IF EXISTS ancestors;';
                }
                $sql .= 'CREATE TABLE IF NOT EXISTS ancestors(woeid INTEGER, ancestors TEXT);';
                break;

            case self::SIBLINGS_TABLE:
                if ($reset) {
                    $sql .= 'DROP TABLE IF EXISTS siblings;';
                }
                $sql .= 'CREATE TABLE IF NOT EXISTS siblings(woeid INTEGER, siblings TEXT);';
                break;

            case self::DESCENDANTS_TABLE:
                if ($reset) {
                    $sql .= 'DROP TABLE IF EXISTS descendants;';
                }
                $sql .= 'CREATE TABLE IF NOT EXISTS descendants(woeid INTEGER, descendants TEXT);';
                break;

            case self::BELONGSTOS_TABLE:
                if ($reset) {
                    $sql .= 'DROP TABLE IF EXISTS belongstos;';
                }
                $sql .= 'CREATE TABLE IF NOT EXISTS belongstos(woeid INTEGER, belongstos TEXT);';
                break;

            case self::COORDS_TABLE:
                if ($reset) {
                    $sql .= 'DROP TABLE IF EXISTS coords;';
                }
                $sql .= 'CREATE TABLE IF NOT EXISTS coords(woeid INTEGER, lon REAL, lat REAL, swlon REAL, swlat REAL, nelon REAL, nelat REAL);';
                break;

            case self::CONCORDANCE_TABLE:
                if ($reset) {
                    $sql .= 'DROP TABLE IF EXISTS concordance;';
                }
                $sql .= 'CREATE TABLE IF NOT EXISTS concordance(woeid INTEGER, geonamesid INTEGER, quattroshapesid INTEGER);';
                break;

            case self::COUNTRIES_TABLE:
                if ($reset) {
                    $sql .= 'DROP TABLE IF EXISTS countries;';
                }
                $sql .= 'CREATE TABLE IF NOT EXISTS countries(woeid INTEGER, name TEXT, iso2 TEXT, iso3 TEXT);';
                break;

            default:
                break;
        }

        if (!empty($sql)) {
            $this->schema_change = true;
            $this->db->beginTransaction();
            if ($this->db->exec($sql) === FALSE) {
                throw new \Exception(var_export($this->db->errorInfo(), true));
            }
            $this->db->commit();
        }
    }

    public function create_index($table) {
        $sql = '';
        switch ($table) {
            case self::META_TABLE:
                $sql .= 'CREATE UNIQUE INDEX IF NOT EXISTS meta_by_id ON meta(id);';
                break;

            case self::PLACES_TABLE:
                $sql .= 'CREATE UNIQUE INDEX IF NOT EXISTS places_by_woeid ON places(woeid);';
                $sql .= 'CREATE INDEX IF NOT EXISTS places_by_parent ON places(parent);';
                break;

            case self::ADJACENCIES_TABLE:
                $sql .= 'CREATE INDEX IF NOT EXISTS adjacencies_by_woeid ON adjacencies(woeid);';
                break;

            case self::ALIASES_TABLE:
                $sql .= 'CREATE INDEX IF NOT EXISTS aliases_by_woeid ON aliases(woeid);';
                break;

            case self::PLACETYPES_TABLE:
                $sql .= 'CREATE INDEX IF NOT EXISTS placetype_by_id ON placetypes(id);';
                $sql .= 'CREATE INDEX IF NOT EXISTS placetype_by_name ON placetypes(shortname);';
                break;

            case self::ADMINS_TABLE:
                $sql .= 'CREATE UNIQUE INDEX IF NOT EXISTS admin_by_id ON admins(woeid);';
                break;

            case self::CHILDREN_TABLE:
                $sql .= 'CREATE UNIQUE INDEX IF NOT EXISTS children_by_woeid ON children(woeid);';
                break;

            case self::ANCESTORS_TABLE:
                $sql .= 'CREATE UNIQUE INDEX IF NOT EXISTS ancestors_by_woeid ON ancestors(woeid);';
                break;

            case self::SIBLINGS_TABLE:
                $sql .= 'CREATE UNIQUE INDEX IF NOT EXISTS siblings_by_woeid ON siblings(woeid);';
                break;

            case self::DESCENDANTS_TABLE:
                $sql .= 'CREATE UNIQUE INDEX IF NOT EXISTS descendants_by_woeid ON descendants(woeid)';
                break;

            case self::BELONGSTOS_TABLE:
                $sql .= 'CREATE UNIQUE INDEX IF NOT EXISTS belongstos_by_woeid ON belongstos(woeid);';
                break;

            case self::COORDS_TABLE:
                $sql .= 'CREATE UNIQUE INDEX IF NOT EXISTS coords_by_woeid ON coords(woeid);';
                break;

            case self::CONCORDANCE_TABLE:
                $sql .= 'CREATE UNIQUE INDEX IF NOT EXISTS concordance_by_woeid ON concordance(woeid);';
                break;

            case self::COUNTRIES_TABLE:
                $sql .= 'CREATE UNIQUE INDEX IF NOT EXISTS countries_by_woeid ON countries(woeid);';
                break;

            default:
                break;
        }

        if (!empty($sql)) {
            $this->schema_change = true;
            $this->db->beginTransaction();
            if ($this->db->exec($sql) === FALSE) {
                throw new \Exception(var_export($this->db->errorInfo(), true));
            }
            $this->db->commit();
        }
    }
}

?>
