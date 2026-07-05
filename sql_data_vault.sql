-- Data Vault model for Rick and Morty data.
-- Source table: stg.characters.

-- Create a separate schema for Data Vault objects.
CREATE SCHEMA IF NOT EXISTS dv;

DROP TABLE IF EXISTS dv.satellite_location_details;
DROP TABLE IF EXISTS dv.satellite_character_details;
DROP TABLE IF EXISTS dv.link_character_species;
DROP TABLE IF EXISTS dv.link_character_current_location;
DROP TABLE IF EXISTS dv.link_character_origin;
DROP TABLE IF EXISTS dv.hub_species;
DROP TABLE IF EXISTS dv.hub_location;
DROP TABLE IF EXISTS dv.hub_character;


-- Hub tables store unique business keys of core entities.
-- They do not store descriptive attributes.
CREATE TABLE dv.hub_character (
    character_hash_key char(32) PRIMARY KEY,
    character_business_key int NOT NULL UNIQUE,
    load_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_source varchar(100) NOT NULL
);

CREATE TABLE dv.hub_location (
    location_hash_key char(32) PRIMARY KEY,
    location_business_key varchar(255) NOT NULL UNIQUE,
    load_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_source varchar(100) NOT NULL
);

CREATE TABLE dv.hub_species (
    species_hash_key char(32) PRIMARY KEY,
    species_business_key varchar(255) NOT NULL UNIQUE,
    load_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_source varchar(100) NOT NULL
);


-- SATELLITES
-- Satellite tables store descriptive attributes of hubs.
CREATE TABLE dv.satellite_character_details (
    character_hash_key char(32) NOT NULL
        REFERENCES dv.hub_character(character_hash_key),
    load_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_source varchar(100) NOT NULL,
    hashdiff char(32) NOT NULL,
    character_name varchar(255),
    status varchar(100),
    gender varchar(100),
    type varchar(255),
    image varchar(500),
    character_url varchar(500),
    created varchar(100),
    episode text,
    PRIMARY KEY (character_hash_key, load_timestamp)
);

CREATE TABLE dv.satellite_location_details (
    location_hash_key char(32) NOT NULL
        REFERENCES dv.hub_location(location_hash_key),
    load_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_source varchar(100) NOT NULL,
    hashdiff char(32) NOT NULL,
    location_name varchar(255),
    location_url varchar(500),
    PRIMARY KEY (location_hash_key, load_timestamp)
);


-- LINKS
-- Link tables store relationships between hubs.
CREATE TABLE dv.link_character_origin (
    character_origin_hash_key char(32) PRIMARY KEY,
    character_hash_key char(32) NOT NULL
        REFERENCES dv.hub_character(character_hash_key),
    location_hash_key char(32) NOT NULL
        REFERENCES dv.hub_location(location_hash_key),
    load_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_source varchar(100) NOT NULL
);

CREATE TABLE dv.link_character_current_location (
    character_current_location_hash_key char(32) PRIMARY KEY,
    character_hash_key char(32) NOT NULL
        REFERENCES dv.hub_character(character_hash_key),
    location_hash_key char(32) NOT NULL
        REFERENCES dv.hub_location(location_hash_key),
    load_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_source varchar(100) NOT NULL
);

CREATE TABLE dv.link_character_species (
    character_species_hash_key char(32) PRIMARY KEY,
    character_hash_key char(32) NOT NULL
        REFERENCES dv.hub_character(character_hash_key),
    species_hash_key char(32) NOT NULL
        REFERENCES dv.hub_species(species_hash_key),
    load_timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
    record_source varchar(100) NOT NULL
);


-- LOAD HUBS
-- Data is loaded from stg.characters.
INSERT INTO dv.hub_character (
    character_hash_key,
    character_business_key,
    load_timestamp,
    record_source
)
SELECT DISTINCT
    md5('character|' || id::text) AS character_hash_key,
    id::int AS character_business_key,
    CURRENT_TIMESTAMP AS load_timestamp,
    'Rick and Morty API' AS record_source
FROM stg.characters
WHERE id IS NOT NULL
ON CONFLICT (character_hash_key) DO NOTHING;

WITH locations AS (
    SELECT
        COALESCE(NULLIF(TRIM(origin_name), ''), 'unknown')
            AS location_business_key
    FROM stg.characters
    UNION
    SELECT
        COALESCE(NULLIF(TRIM(location_name), ''), 'unknown')
            AS location_business_key
    FROM stg.characters
)
INSERT INTO dv.hub_location (
    location_hash_key,
    location_business_key,
    load_timestamp,
    record_source
)
SELECT DISTINCT
    md5('location|' || LOWER(location_business_key)) AS location_hash_key,
    location_business_key,
    CURRENT_TIMESTAMP AS load_timestamp,
    'Rick and Morty API' AS record_source
FROM locations
ON CONFLICT (location_hash_key) DO NOTHING;

INSERT INTO dv.hub_species (
    species_hash_key,
    species_business_key,
    load_timestamp,
    record_source
)
SELECT DISTINCT
    md5(
        'species|' || LOWER(COALESCE(NULLIF(TRIM(species), ''), 'unknown'))
    ) AS species_hash_key,
    COALESCE(NULLIF(TRIM(species), ''), 'unknown') AS species_business_key,
    CURRENT_TIMESTAMP AS load_timestamp,
    'Rick and Morty API' AS record_source
FROM stg.characters
ON CONFLICT (species_hash_key) DO NOTHING;


-- LOAD SATELLITES
-- Satellite rows are inserted only if the same hashdiff
INSERT INTO dv.satellite_character_details (
    character_hash_key,
    load_timestamp,
    record_source,
    hashdiff,
    character_name,
    status,
    gender,
    type,
    image,
    character_url,
    created,
    episode
)
SELECT
    md5('character|' || id::text) AS character_hash_key,
    CURRENT_TIMESTAMP AS load_timestamp,
    'Rick and Morty API' AS record_source,
    md5(
        COALESCE(name, '') || '|' ||
        COALESCE(status, '') || '|' ||
        COALESCE(gender, '') || '|' ||
        COALESCE(type, '') || '|' ||
        COALESCE(image, '') || '|' ||
        COALESCE(url, '') || '|' ||
        COALESCE(created, '') || '|' ||
        COALESCE(episode, '')
    ) AS hashdiff,
    name AS character_name,
    status,
    gender,
    type,
    image,
    url AS character_url,
    created,
    episode
FROM stg.characters src
WHERE id IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM dv.satellite_character_details sat
      WHERE sat.character_hash_key = md5('character|' || src.id::text)
        AND sat.hashdiff = md5(
            COALESCE(src.name, '') || '|' ||
            COALESCE(src.status, '') || '|' ||
            COALESCE(src.gender, '') || '|' ||
            COALESCE(src.type, '') || '|' ||
            COALESCE(src.image, '') || '|' ||
            COALESCE(src.url, '') || '|' ||
            COALESCE(src.created, '') || '|' ||
            COALESCE(src.episode, '')
        )
  );

WITH location_details AS (
    SELECT
        COALESCE(NULLIF(TRIM(origin_name), ''), 'unknown')
            AS location_business_key,
        NULLIF(TRIM(origin_url), '') AS location_url
    FROM stg.characters
    UNION ALL
    SELECT
        COALESCE(NULLIF(TRIM(location_name), ''), 'unknown')
            AS location_business_key,
        NULLIF(TRIM(location_url), '') AS location_url
    FROM stg.characters
),
location_grouped AS (
    SELECT
        location_business_key,
        MAX(location_url) AS location_url
    FROM location_details
    GROUP BY location_business_key
)
INSERT INTO dv.satellite_location_details (
    location_hash_key,
    load_timestamp,
    record_source,
    hashdiff,
    location_name,
    location_url
)
SELECT
    md5('location|' || LOWER(location_business_key)) AS location_hash_key,
    CURRENT_TIMESTAMP AS load_timestamp,
    'Rick and Morty API' AS record_source,
    md5(
        COALESCE(location_business_key, '') || '|' ||
        COALESCE(location_url, '')
    ) AS hashdiff,
    location_business_key AS location_name,
    location_url
FROM location_grouped src
WHERE NOT EXISTS (
    SELECT 1
    FROM dv.satellite_location_details sat
    WHERE sat.location_hash_key = md5(
        'location|' || LOWER(src.location_business_key)
    )
      AND sat.hashdiff = md5(
          COALESCE(src.location_business_key, '') || '|' ||
          COALESCE(src.location_url, '')
      )
);

-- LOAD LINKS
-- Each link connects a character with another business entity.
INSERT INTO dv.link_character_origin (
    character_origin_hash_key,
    character_hash_key,
    location_hash_key,
    load_timestamp,
    record_source
)
SELECT DISTINCT
    md5(
        'character_origin|' ||
        id::text || '|' ||
        LOWER(COALESCE(NULLIF(TRIM(origin_name), ''), 'unknown'))
    ) AS character_origin_hash_key,
    md5('character|' || id::text) AS character_hash_key,
    md5(
        'location|' ||
        LOWER(COALESCE(NULLIF(TRIM(origin_name), ''), 'unknown'))
    ) AS location_hash_key,
    CURRENT_TIMESTAMP AS load_timestamp,
    'Rick and Morty API' AS record_source
FROM stg.characters
WHERE id IS NOT NULL
ON CONFLICT (character_origin_hash_key) DO NOTHING;

INSERT INTO dv.link_character_current_location (
    character_current_location_hash_key,
    character_hash_key,
    location_hash_key,
    load_timestamp,
    record_source
)
SELECT DISTINCT
    md5(
        'character_current_location|' ||
        id::text || '|' ||
        LOWER(COALESCE(NULLIF(TRIM(location_name), ''), 'unknown'))
    ) AS character_current_location_hash_key,
    md5('character|' || id::text) AS character_hash_key,
    md5(
        'location|' ||
        LOWER(COALESCE(NULLIF(TRIM(location_name), ''), 'unknown'))
    ) AS location_hash_key,
    CURRENT_TIMESTAMP AS load_timestamp,
    'Rick and Morty API' AS record_source
FROM stg.characters
WHERE id IS NOT NULL
ON CONFLICT (character_current_location_hash_key) DO NOTHING;

INSERT INTO dv.link_character_species (
    character_species_hash_key,
    character_hash_key,
    species_hash_key,
    load_timestamp,
    record_source
)
SELECT DISTINCT
    md5(
        'character_species|' ||
        id::text || '|' ||
        LOWER(COALESCE(NULLIF(TRIM(species), ''), 'unknown'))
    ) AS character_species_hash_key,
    md5('character|' || id::text) AS character_hash_key,
    md5(
        'species|' ||
        LOWER(COALESCE(NULLIF(TRIM(species), ''), 'unknown'))
    ) AS species_hash_key,
    CURRENT_TIMESTAMP AS load_timestamp,
    'Rick and Morty API' AS record_source
FROM stg.characters
WHERE id IS NOT NULL
ON CONFLICT (character_species_hash_key) DO NOTHING;


-- VALIDATION QUERY
-- Shows the number of rows loaded into each Data Vault table.
SELECT 'hub_character' AS table_name, COUNT(*) AS row_count
FROM dv.hub_character
UNION ALL
SELECT 'hub_location', COUNT(*)
FROM dv.hub_location
UNION ALL
SELECT 'hub_species', COUNT(*)
FROM dv.hub_species
UNION ALL
SELECT 'satellite_character_details', COUNT(*)
FROM dv.satellite_character_details
UNION ALL
SELECT 'satellite_location_details', COUNT(*)
FROM dv.satellite_location_details
UNION ALL
SELECT 'link_character_origin', COUNT(*)
FROM dv.link_character_origin
UNION ALL
SELECT 'link_character_current_location', COUNT(*)
FROM dv.link_character_current_location
UNION ALL
SELECT 'link_character_species', COUNT(*)
FROM dv.link_character_species;

-- Reconstruct a readable character dataset from the Data Vault model.
-- The query joins the character hub with its satellite attributes
SELECT
    hc.character_business_key AS character_id,
    scd.character_name,
    scd.status,
    scd.gender,
    hs.species_business_key AS species,
    hlo.location_business_key AS origin_location,
    hlc.location_business_key AS current_location
FROM dv.hub_character hc
JOIN dv.satellite_character_details scd
    ON hc.character_hash_key = scd.character_hash_key
LEFT JOIN dv.link_character_species lcs
    ON hc.character_hash_key = lcs.character_hash_key
LEFT JOIN dv.hub_species hs
    ON lcs.species_hash_key = hs.species_hash_key
LEFT JOIN dv.link_character_origin lco
    ON hc.character_hash_key = lco.character_hash_key
LEFT JOIN dv.hub_location hlo
    ON lco.location_hash_key = hlo.location_hash_key
LEFT JOIN dv.link_character_current_location lccl
    ON hc.character_hash_key = lccl.character_hash_key
LEFT JOIN dv.hub_location hlc
    ON lccl.location_hash_key = hlc.location_hash_key
ORDER BY hc.character_business_key
LIMIT 10;
