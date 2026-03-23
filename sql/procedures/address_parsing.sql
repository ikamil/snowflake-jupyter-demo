-- =============================================================================
-- Address Parsing & Normalization (sample)
-- Demonstrates: complex CASE expressions, REGEXP_REPLACE, TRANSLATE,
--   SPLIT_PART, RLIKE regex matching, chained string transformations
-- =============================================================================

-- Multi-step address normalization UDF
CREATE OR REPLACE FUNCTION PUBLIC.normalize_address(raw_addr VARCHAR)
RETURNS VARCHAR
IMMUTABLE
AS $$
    UPPER(TRIM(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(raw_addr, '\\s+', ' '),   -- collapse whitespace
                '#\\s*', 'UNIT '                           -- normalize unit prefix
            ),
            '\\.$', ''                                     -- remove trailing period
        )
    ))
$$;

-- Detect address suffix type from free-text address field
-- Pattern: multi-condition CASE with regex for classification
SELECT
    address_raw,
    normalize_address(address_raw) AS address_clean,
    CASE
        WHEN RLIKE(address_raw, '.*\\b(APT|APARTMENT)\\s*#?\\d+.*', 'i') THEN 'APARTMENT'
        WHEN RLIKE(address_raw, '.*\\b(STE|SUITE)\\s*#?\\d+.*', 'i')     THEN 'SUITE'
        WHEN RLIKE(address_raw, '.*\\b(UNIT)\\s*#?[A-Z0-9]+.*', 'i')     THEN 'UNIT'
        WHEN RLIKE(address_raw, '.*\\bPO\\s*BOX\\s*\\d+.*', 'i')         THEN 'PO_BOX'
        ELSE 'SINGLE_FAMILY'
    END AS address_type,
    SPLIT_PART(address_raw, ' ', 1)                                       AS street_number,
    TRANSLATE(SPLIT_PART(address_raw, ' ', 1), '0123456789', '')          AS non_numeric_check
FROM staging.raw_addresses
WHERE LENGTH(TRIM(address_raw)) > 0;
