{{ config (
    alias = target.database + '_facebook_ad_performance'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_effective_status,
campaign_type_default,
CASE WHEN campaign_name ~* 'Retargeting' THEN 'Retargeting' 
    WHEN campaign_name ~* 'Prospecting' OR campaign_name ~* 'ASC' THEN 'Prospecting'
END as tactic,
adset_name,
adset_id,
adset_effective_status,
audience,
ad_name,
ad_id,
ad_effective_status,
visual,
copy,
format_visual,
visual_copy,
date,
date_granularity,
spend,
impressions,
link_clicks,
add_to_cart,
purchases,
purchases_1_d_view,
purchases_7_d_click,
revenue,
revenue_1_d_view,
revenue_7_d_click
FROM {{ ref('facebook_performance_by_ad') }}
