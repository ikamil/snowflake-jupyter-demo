-- =============================================================================
-- Household Graph Construction (sample)
-- Demonstrates: multi-source UNION in CTE, UPDATE ... SET ... FROM (join update),
--   regex validation with RLIKE, CREATE TABLE AS with complex joins,
--   MAX aggregation for deduplication, zip+4+2 postal matching
-- =============================================================================

-- Build extended postal codes for household matching
UPDATE analytics.household_records
SET postal_code_ext = zip5 || zip4 || zip2
WHERE (postal_code_ext IS NULL OR postal_code_ext = '')
  AND RLIKE(zip5, '[0-9]{5}')
  AND RLIKE(zip4, '[0-9]{4}')
  AND RLIKE(zip2, '[0-9]{2}');

-- Match records to household graph via postal code
UPDATE analytics.household_records tgt
SET household_id = ref.household_id
FROM reference.postal_to_household ref
WHERE ref.postal_code_ext = tgt.postal_code_ext
  AND tgt.household_id IS NULL;

-- Second-pass matching from supplementary source
UPDATE analytics.household_records tgt
SET household_id = sup.household_id
FROM reference.supplementary_households sup
WHERE sup.postal_code_ext = tgt.postal_code_ext
  AND tgt.household_id IS NULL;

-- Resolve household → parcel linkage
UPDATE analytics.household_records tgt
SET parcel_id = ref.parcel_id
FROM reference.household_to_parcel ref
WHERE ref.household_id = tgt.household_id
  AND tgt.parcel_id IS NULL;

-- Generate new household IDs for unmatched records
CREATE TABLE staging.new_households (household_id, postal_code_ext, state, zip5, city, address_primary, address_secondary, source) AS
WITH id_max AS (
    SELECT MAX(household_id) max_id
    FROM (
        SELECT household_id FROM reference.supplementary_households
        UNION ALL
        SELECT household_id FROM reference.postal_to_household
    )
),
unmatched AS (
    SELECT
        postal_code_ext,
        MAX(UPPER(TRIM(state)))      AS state,
        MIN(CAST(zip5 AS INT))       AS zip5,
        MAX(UPPER(TRIM(city)))       AS city,
        MIN(TRIM(UPPER(address_1)))  AS address_primary,
        MIN(TRIM(UPPER(address_2)))  AS address_secondary,
        'household_quarterly'        AS source
    FROM analytics.household_records
    WHERE household_id IS NULL AND postal_code_ext IS NOT NULL
    GROUP BY postal_code_ext
)
SELECT
    id_max.max_id + ROW_NUMBER() OVER (ORDER BY postal_code_ext) AS household_id,
    u.*
FROM unmatched u
    CROSS JOIN id_max;
