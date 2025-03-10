{{ config (
    alias = target.database + '_googleads_campaign_performance'
)}}

SELECT 
account_id,
campaign_name,
campaign_id,
campaign_status,
campaign_type_default,
CASE WHEN campaign_name ~* 'Search' AND campaign_name ~* 'Brand' THEN 'Brand Search'
    WHEN campaign_name ~* 'Search' AND campaign_name ~* 'NB' THEN 'Non-Brand Search'
    WHEN campaign_name ~* 'Demand' THEN 'Demand Gen'
    ELSE 'Shopping'
END AS tactic,
CASE WHEN (campaign_name ~* 'PMax' OR campaign_name ~* 'Shopping') AND campaign_name ~* 'Brand' THEN 'Brand PMax/Shopping'
    WHEN (campaign_name ~* 'PMax' OR campaign_name ~* 'Shopping') AND campaign_name !~* 'Brand' THEN 'NB PMax/Shopping'
    WHEN campaign_name ~* 'Search' AND campaign_name ~* 'Brand' THEN 'Brand Search'
    WHEN campaign_name ~* 'Search' AND campaign_name ~* 'NB' THEN 'Non-Brand Search'
    WHEN campaign_name ~* 'Demand' THEN 'Demand Gen'
    ELSE 'Others'
END AS tactic_granular,
date,
date_granularity,
spend,
impressions,
clicks,
"[custompixel]googleadspurchaseall" as purchases,
"[custompixel]googleadspurchaseall_value" as revenue,
googleadsgiftpurchaseconversion as gift_purchases,
googleadsgiftpurchaseconversion_value as gift_purchases_revenue,
googleadssubscriptionpurchaseconversion as subscription_purchases,
googleadssubscriptionpurchaseconversion_value as subscription_purchases_revenue,
googleadsalcpurchaseconversion as alc_purchases,
googleadsalcpurchaseconversion_value as alc_purchases_revenue,
search_impression_share,
search_budget_lost_impression_share,
search_rank_lost_impression_share,
quizformcompletions as quiz_completions,
quizformstarts as quiz_starts
FROM {{ ref('googleads_performance_by_campaign') }}
WHERE date < current_date
