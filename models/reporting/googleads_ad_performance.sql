{{ config (
    alias = target.database + '_googleads_ad_performance'
)}}

SELECT
account_id,
ad_id,
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
CASE WHEN campaign_name ~* 'Search' AND campaign_name ~* 'Brand' THEN 'Brand Search'
    WHEN campaign_name ~* 'Search' AND campaign_name ~* 'NB' THEN 'Non-Brand Search'
    WHEN campaign_name ~* 'Demand' THEN 'Demand Gen'
    ELSE 'Shopping'
END AS tactic,
ad_group_name,
ad_group_id,
date,
date_granularity,
spend,
impressions,
clicks,
conversions as purchases,
conversions_value as revenue,
googleadsgiftpurchaseconversion as gift_purchases,
googleadsgiftpurchaseconversion_value as gift_purchases_revenue,
googleadssubscriptionpurchaseconversion as subscription_purchases,
googleadssubscriptionpurchaseconversion_value as subscription_purchases_revenue,
googleadsalcpurchaseconversion as alc_purchases,
googleadsalcpurchaseconversion_value as alc_purchases_revenue

FROM {{ ref('googleads_performance_by_ad') }}
WHERE date < current_date
