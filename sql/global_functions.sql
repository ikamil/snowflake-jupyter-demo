-- =============================================================================
-- Global UDFs & Stored Procedures
-- Demonstrates: Snowflake scalar UDFs, haversine distance, JS stored procedures
-- =============================================================================

-- Haversine distance between two lat/lon points (returns meters)
CREATE OR REPLACE FUNCTION PUBLIC.calc_distance(
    lat1 FLOAT, lon1 FLOAT, lat2 FLOAT, lon2 FLOAT
) RETURNS FLOAT IMMUTABLE AS $$
    12742000 * ASIN(SQRT(
        0.5 - COS(((lat1) - (lat2)) * PI()/180) / 2
        + COS((lat2) * PI()/180) * COS((lat1) * PI()/180)
          * (1 - COS(((lon1) - (lon2)) * PI()/180)) / 2
    ))
$$;

-- Distance from lat/lon to a grid cell center (offset-based)
CREATE OR REPLACE FUNCTION PUBLIC.calc_distance_offset(
    lat FLOAT, lon FLOAT,
    offset_lat INT, offset_lon INT,
    accuracy INT
) RETURNS FLOAT IMMUTABLE AS $$
    calc_distance(
        lat, lon,
        POWER(0.1, accuracy) * (FLOOR(lat * POWER(10, accuracy) + offset_lat) + 0.5),
        POWER(0.1, accuracy) * (FLOOR(lon * POWER(10, accuracy) + offset_lon) + 0.5)
    )
$$;

-- Proximity score: converts feet distance to a tier (3=close, 4=far)
CREATE OR REPLACE FUNCTION PUBLIC.proximity_score(
    feet DECIMAL, context TEXT, ref_date DATE
) RETURNS INT IMMUTABLE AS $$
    CASE WHEN feet <= 65 THEN 3 ELSE 4 END
$$;


-- JavaScript stored procedure: validates results after a weekly aggregation
CREATE OR REPLACE PROCEDURE validate_weekly_result(
    week_start VARCHAR, week_end VARCHAR, threshold FLOAT
)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS $$
    var cmd = "SELECT COUNT(1) total_records, \
        COUNT(DISTINCT CASE WHEN segment = 'segment_a' THEN device_id END) seg_a, \
        COUNT(DISTINCT CASE WHEN segment = 'segment_b' THEN device_id END) seg_b, \
        COUNT(DISTINCT CASE WHEN segment = 'segment_c' THEN device_id END) seg_c \
        FROM analytics.weekly_segments WHERE week_start = :1";

    var stmt = snowflake.createStatement({sqlText: cmd, binds: [WEEK_START]});
    var result = stmt.execute();
    result.next();

    return 'Records: ' + result.getColumnValue(1)
         + '; seg_a: ' + result.getColumnValue(2)
         + '; seg_b: ' + result.getColumnValue(3)
         + '; seg_c: ' + result.getColumnValue(4);
$$;
