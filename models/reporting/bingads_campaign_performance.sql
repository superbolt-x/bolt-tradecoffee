{{ config (
    alias = target.database + '_bingads_campaign_performance'
)}}

SELECT 
account_id,
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
CASE WHEN campaign_name ~* 'Search' AND campaign_name ~* 'Brand' THEN 'Brand Search'
    WHEN campaign_name ~* 'Search' AND campaign_name ~* 'NB' THEN 'Non-Brand Search'
    WHEN campaign_name ~* 'Shopping' AND campaign_name ~* 'Brand' THEN 'Brand Shopping'
    WHEN campaign_name ~* 'Shopping' AND campaign_name ~* 'NB' THEN 'Non-Brand Shopping'
    ELSE 'Other'
END AS tactic,
date,
date_granularity,
spend,
impressions,
clicks,
conversions as purchases,
revenue,
view_through_conversions
FROM {{ ref('bingads_performance_by_campaign') }}
