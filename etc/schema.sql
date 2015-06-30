-- Table: adjacencies
DROP TABLE IF EXISTS adjacencies;
CREATE TABLE IF NOT EXISTS adjacencies (
    woeid integer NOT NULL,
    placetype smallint NOT NULL,
    adjacent integer NOT NULL,
    CONSTRAINT adjacency_by_woeid PRIMARY KEY (woeid)
)
WITH (
    OIDS=FALSE
);
ALTER TABLE adjacencies OWNER TO gary;

-- Index: adjacency_by_placetype
CREATE INDEX adjacency_by_placetype ON adjacencies USING btree (placetype);

-- Table: admins
DROP TABLE IF EXISTS admins;
CREATE TABLE IF NOT EXISTS admins (
    woeid integer NOT NULL,
    state integer,
    county integer,
    localadmin integer,
    country integer,
    continent integer,
    CONSTRAINT admins_by_woeid PRIMARY KEY (woeid)
)
WITH (
    OIDS=FALSE
);
ALTER TABLE admins OWNER TO gary;

-- Table: aliases
DROP TABLE IF EXISTS aliases;
CREATE TABLE IF NOT EXISTS aliases (
    woeid integer NOT NULL,
    type character(1) NOT NULL,
    lang character(3) NOT NULL,
    name character varying NOT NULL,
    CONSTRAINT aliases_by_woeid PRIMARY KEY (woeid)
)
WITH (
    OIDS=FALSE
);
ALTER TABLE aliases OWNER TO gary;

-- Index: aliases_by_type
DROP INDEX IF EXISTS aliases_by_type;
CREATE INDEX aliases_by_type ON aliases USING btree(type COLLATE pg_catalog."default");

-- Index: aliases_by_lang
DROP INDEX IF EXISTS aliases_by_lang;
CREATE INDEX aliases_by_lang ON aliases USING btree(lang COLLATE pg_catalog."default");

-- Index: aliases_by_name
DROP INDEX IF EXISTS aliases_by_name;
CREATE INDEX aliases_by_name ON aliases USING btree(name);

-- Table: places
DROP TABLE IF EXISTS places;
CREATE TABLE IF NOT EXISTS places (
    woeid integer NOT NULL,
    iso character varying,
    name character varying,
    lang character varying NOT NULL,
    placetype smallint NOT NULL,
    placetypename character varying NOT NULL,
    centroid geography(Point,4326),
    bbox box,
    bounds geography(Polygon,4326),
    parent integer,
    history character varying[],
    CONSTRAINT place_by_woeid PRIMARY KEY (woeid)
)
WITH (
    OIDS=FALSE
);
ALTER TABLE places OWNER TO gary;

-- Table: placetypes
DROP TABLE IF EXISTS placetypes;
CREATE TABLE IF NOT EXISTS placetypes (
  id smallint NOT NULL,
  name character varying NOT NULL,
  descr character varying NOT NULL,
  shortname character varying NOT NULL,
  tag character varying NOT NULL,
  CONSTRAINT placetype_by_id PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE placetypes OWNER TO gary;

-- Index: placetype_by_name
DROP INDEX IF EXISTS placetype_by_name;
CREATE INDEX placetype_by_name ON placetypes USING btree (name COLLATE pg_catalog."default");

-- Index: placetype_by_shortname
DROP INDEX IF EXISTS placetype_by_shortname;
CREATE INDEX placetype_by_shortname ON placetypes USING btree (shortname COLLATE pg_catalog."default");

-- Index: placetype_by_tag
DROP INDEX IF EXISTS placetype_by_tag;
CREATE INDEX placetype_by_tag ON placetypes USING btree (tag COLLATE pg_catalog."default");
