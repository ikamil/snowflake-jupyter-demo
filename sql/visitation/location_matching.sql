-- =============================================================================
-- Location List Management & Grid Matching (sample)
-- Demonstrates: UPDATE ... FROM (join-based update), correlated NOT EXISTS,
--   ALTER TABLE, TRUNCATE, transient tables, COALESCE/GREATEST for null handling
-- =============================================================================

-- Sync location list status from upstream source
UPDATE staging.location_lists tgt
SET status = src.status
FROM upstream.location_feed src
WHERE tgt.list_id = src.list_id
  AND COALESCE(tgt.status, '') <> COALESCE(src.status, '')
  AND src.status IS NOT NULL;

-- Find lists that need reprocessing (updated since last load)
SELECT *
FROM staging.location_lists s
    LEFT JOIN staging.location_lists_snapshot p ON s.list_id = p.list_id
WHERE s.status = 'OK'
  AND COALESCE(s.load_date, p.load_date, '2000-01-01')
    < COALESCE(GREATEST(s.last_update, p.last_update), s.load_date, p.load_date, '2000-01-02');

-- Detect locations missing grid-cell mappings
SELECT *
FROM staging.location_lists
WHERE list_id IN (
    SELECT DISTINCT list_id
    FROM staging.location_points p
    WHERE NOT EXISTS (
        SELECT 1 FROM staging.grid_cell_map g WHERE g.point_id = p.point_id
    )
);

-- Rebuild grid cells for a specific location list
TRUNCATE TABLE staging.grid_cell_map_rebuild;

INSERT INTO staging.grid_cell_map_rebuild
SELECT
    grid_encode(p.lat, p.lon, b.offset_lat, b.offset_lon, 4) AS grid_cell_id,
    p.point_id,
    p.list_id,
    calc_distance_offset(p.lat, p.lon, b.offset_lat, b.offset_lon, 4) AS distance_m
FROM staging.location_points p
    CROSS JOIN TABLE(grid_offsets_radius(16)) AS b
WHERE p.list_id = 1528
  AND grid_circle_intersects(p.lat, p.lon, b.offset_lat, b.offset_lon, 4, 91.44::FLOAT);

-- Schema evolution: add column for incremental processing
ALTER TABLE staging.location_lists ADD COLUMN IF NOT EXISTS process_step VARCHAR(100);
