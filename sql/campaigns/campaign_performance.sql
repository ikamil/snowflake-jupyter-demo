-- =============================================================================
-- Campaign Performance Report (sample — recurring monthly pattern)
-- Demonstrates: CTEs, EXISTS/NOT EXISTS filtering, date arithmetic,
--   conditional aggregation with CASE, INSERT INTO ... SELECT
-- =============================================================================

-- Monthly performance snapshot using CTEs for readability
WITH active_campaigns AS (
    SELECT campaign_id, campaign_name, start_date, end_date
    FROM config.campaigns
    WHERE status = 'active'
      AND start_date <= CURRENT_DATE
),

daily_impressions AS (
    SELECT
        campaign_id,
        impression_date,
        COUNT(*)                                           AS impressions,
        COUNT(DISTINCT device_id)                          AS unique_devices,
        SUM(CASE WHEN channel = 'mobile'  THEN 1 ELSE 0 END) AS mobile_count,
        SUM(CASE WHEN channel = 'desktop' THEN 1 ELSE 0 END) AS desktop_count
    FROM events.impressions i
    WHERE EXISTS (
        SELECT 1 FROM active_campaigns ac
        WHERE ac.campaign_id = i.campaign_id
    )
    GROUP BY campaign_id, impression_date
),

visit_attribution AS (
    SELECT
        v.campaign_id,
        COUNT(DISTINCT v.device_id) AS attributed_visits
    FROM events.store_visits v
    WHERE NOT EXISTS (
        SELECT 1 FROM exclusions.suppression_list s
        WHERE s.device_id = v.device_id
    )
    GROUP BY v.campaign_id
)

-- Final insert into monthly report table
INSERT INTO reports.monthly_performance
SELECT
    ac.campaign_id,
    ac.campaign_name,
    SUM(di.impressions)     AS total_impressions,
    SUM(di.unique_devices)  AS total_reach,
    SUM(di.mobile_count)    AS mobile_impressions,
    SUM(di.desktop_count)   AS desktop_impressions,
    va.attributed_visits,
    CURRENT_DATE            AS report_date
FROM active_campaigns ac
    LEFT JOIN daily_impressions di ON di.campaign_id = ac.campaign_id
    LEFT JOIN visit_attribution va ON va.campaign_id = ac.campaign_id
GROUP BY ac.campaign_id, ac.campaign_name, va.attributed_visits;
