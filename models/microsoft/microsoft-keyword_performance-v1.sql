{{
  custom_config(
    alias=var('microsoft_keyword_performance_v1_alias','microsoft-keyword_performance-v1-test'),
    field="TimePeriod")
}}

SELECT
    SAFE_CAST(device_type AS STRING) DeviceType,
    SAFE_CAST(campaign_id AS STRING) CampaignId,
    SAFE_CAST(NULL AS STRING) AdGroupName,
    SAFE_CAST(conversions AS INT64)	Conversions,
    SAFE_CAST(date AS DATE)	TimePeriod,
    SAFE_CAST(NULL AS STRING) AccountName,
    SAFE_CAST(ad_group_id AS STRING) AdGroupId,
    SAFE_CAST(clicks AS INT64) Clicks,
    SAFE_CAST(impressions AS INT64)	Impressions,
    SAFE_CAST(NULL AS STRING) CampaignName,
    SAFE_CAST(revenue AS FLOAT64) Revenue,
    SAFE_CAST(spend AS FLOAT64)	Spend,
    SAFE_CAST(account_id AS STRING)	AccountId,
    SAFE_CAST(NULL AS STRING) Keyword,
    SAFE_CAST(NULL AS STRING) KeywordId,
    SAFE_CAST(NULL AS STRING) AdGroupStatus,
    SAFE_CAST(NULL AS STRING) BidStrategyType,
    SAFE_CAST(NULL AS INT64) AllConversions,
    SAFE_CAST(TRIM(currency_code) AS STRING) CurrencyCode,
    SAFE_CAST(NULL AS STRING) FinalUrl,
    SAFE_CAST(NULL AS STRING) FinalUrlSuffix,
    SAFE_CAST(NULL AS STRING) KeywordLabels,
    SAFE_CAST(network AS STRING) Network,
    SAFE_CAST(view_through_conversions AS INT64) ViewThroughConversions,
    SAFE_CAST(NULL AS STRING) AccountNumber,
    SAFE_CAST(all_revenue AS FLOAT64) AllRevenue,
    SAFE_CAST(keyword_status AS STRING)	KeywordStatus,
    SAFE_CAST(quality_score AS INT64) QualityScore,
    SAFE_CAST(average_position AS FLOAT64) AveragePosition,
    SAFE_CAST(NULL AS STRING) Goal,
 FROM {{ source('microsoft', 'keyword_performance_daily_report') }}