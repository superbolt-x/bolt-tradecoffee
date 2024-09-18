{{ config (
    alias = target.database + '_blended_performance'
)}}

WITH paid_data as
    (SELECT channel, date::date, date_granularity, COALESCE(SUM(spend),0) as spend, COALESCE(SUM(clicks),0) as clicks, COALESCE(SUM(impressions),0) as impressions, 
        COALESCE(SUM(purchases),0) as purchases, COALESCE(SUM(revenue),0) as revenue
    FROM
        (SELECT 'Meta' as channel, date, date_granularity, 
            spend, link_clicks as clicks, impressions, purchases, revenue
        FROM {{ source('reporting','facebook_ad_performance') }}
        UNION ALL
        SELECT 'Google Ads' as channel, date, date_granularity,
            spend, clicks, impressions, purchases, revenue
        FROM {{ source('reporting','googleads_campaign_performance') }}
        /*UNION ALL
        SELECT 'Bing' as channel, date, date_granularity,
            spend, clicks, impressions, purchases, revenue
        FROM {{ source('reporting','bingads_campaign_performance') }}*/
        )
    GROUP BY channel, date, date_granularity)
    
SELECT channel,
    date,
    date_granularity,
    spend,
    clicks,
    impressions,
    purchases,
    revenue
FROM paid_data
