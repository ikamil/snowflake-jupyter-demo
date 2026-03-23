-- =============================================================================
-- Campaign Delivery Pipeline (sample)
-- Demonstrates: temp tables, CTAS, CROSS JOIN for grid expansion,
--   UDF-based distance scoring, clustering, multi-step transformations,
--   window functions, ROLLUP aggregation, UNION ALL
-- =============================================================================

-- Step 1: Build grid-cell reference for target locations
-- CROSS JOIN expands each location into surrounding grid cells via a table function
CREATE OR REPLACE TEMPORARY TABLE staging.campaign_grid_cells
    CLUSTER BY (grid_cell_id) AS
SELECT
    b.offset_lat,
    b.offset_lon,
    grid_encode(loc.lat, loc.lon, b.offset_lat, b.offset_lon, 4)  AS grid_cell_id,
    loc.location_id,
    loc.region_id,
    UPPER(TRIM(loc.location_name))                                 AS location_name,
    loc.location_id                                                AS unique_id,
    loc.list_id,
    loc.store_code,
    UPPER(TRIM(COALESCE(loc.address_1,'') || ' ' || COALESCE(loc.address_2,''))) AS full_address,
    UPPER(TRIM(loc.city))   AS city,
    loc.state,
    loc.zip,
    loc.region_name,
    proximity_score(
        calc_distance_offset(loc.lat, loc.lon, b.offset_lat, b.offset_lon, 4) * 3.28084,
        'campaign_delivery',
        CURRENT_DATE
    ) AS proximity_tier,
    2 AS visit_flag
FROM staging.target_locations loc
    LEFT JOIN staging.location_lists ll ON ll.id = loc.list_id
    CROSS JOIN TABLE(grid_offsets_radius(14)) AS b
WHERE loc.status IN (0, 2, 3, 5)
  AND loc.list_id IN (5627)
  AND grid_circle_intersects(loc.lat, loc.lon, b.offset_lat, b.offset_lon, 4, 91.44::FLOAT);


-- Step 2: Join device observations against grid cells
CREATE OR REPLACE TEMPORARY TABLE staging.campaign_matches
    CLUSTER BY (device_id) AS
SELECT
    d.device_id,
    d.observation_ts,
    d.grid_cell_id,
    g.location_name,
    g.unique_id,
    g.region_name,
    g.proximity_tier,
    ROW_NUMBER() OVER (
        PARTITION BY d.device_id, g.unique_id
        ORDER BY d.observation_ts
    ) AS visit_sequence
FROM observations.device_signals d
    INNER JOIN staging.campaign_grid_cells g
        ON d.grid_cell_id = g.grid_cell_id
WHERE d.observation_date BETWEEN '2020-01-01' AND '2020-01-31'
  AND d.source_type = 'primary';


-- Step 3: Deduplicate — keep first visit per device per location
CREATE OR REPLACE TEMPORARY TABLE staging.campaign_unique_visits AS
SELECT *
FROM staging.campaign_matches
WHERE visit_sequence = 1;


-- Step 4: Aggregate with ROLLUP for subtotals
SELECT
    region_name,
    location_name,
    COUNT(DISTINCT device_id) AS unique_devices,
    COUNT(*)                  AS total_visits,
    MIN(observation_ts)       AS first_visit,
    MAX(observation_ts)       AS last_visit
FROM staging.campaign_unique_visits
GROUP BY ROLLUP (region_name, location_name)
ORDER BY region_name NULLS LAST, location_name NULLS LAST;


-- Step 5: Combine multiple audience sources via UNION ALL
CREATE OR REPLACE TABLE delivery.campaign_audience AS
SELECT device_id, 'visit_based'  AS source FROM staging.campaign_unique_visits
UNION ALL
SELECT device_id, 'ip_matched'   AS source FROM staging.ip_matched_devices
UNION ALL
SELECT device_id, 'lookalike'    AS source FROM staging.lookalike_devices;
