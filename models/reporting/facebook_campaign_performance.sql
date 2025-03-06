{{ config (
    alias = target.database + '_facebook_campaign_performance'
)}}

SELECT 
campaign_name,
campaign_id,
campaign_effective_status,
campaign_type_default,
CASE WHEN campaign_name ~* 'Retargeting' THEN 'Retargeting' 
    WHEN campaign_name ~* 'Prospecting' OR campaign_name ~* 'ASC' OR campaign_name ~* 'Purchase' THEN 'Prospecting'
    ELSE 'TOF'
END as tactic,
date,
date_granularity,
spend,
impressions,
link_clicks,
add_to_cart,
add_to_cart_1_d_view,
add_to_cart_7_d_click,
initiate_checkout,
purchases,
purchases_1_d_view,
purchases_7_d_click,
revenue,
revenue_1_d_view,
revenue_7_d_click
FROM {{ ref('facebook_performance_by_campaign') }}
LEFT JOIN 
    (SELECT campaign_id, date, COALESCE(SUM(_7_d_click),0) as add_to_cart_7_d_click, COALESCE(SUM(_1_d_view),0) as add_to_cart_1_d_view 
    FROM {{ source('facebook_raw','campaigns_insights_actions') }}
    WHERE action_type = 'add_to_cart'
    GROUP BY 1,2) USING (campaign_id,date)
WHERE date < current_date
