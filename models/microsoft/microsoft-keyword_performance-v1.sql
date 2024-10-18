{{
  custom_config(
    alias=var('microsoft_keyword_performance_v1_alias','microsoft-keyword_performance-v1'),
    field="TimePeriod")
}}

SELECT
  SAFE_CAST(device_type AS STRING) DeviceType,
  SAFE_CAST(A.campaign_id AS STRING) CampaignId,
  SAFE_CAST(ad_group_name AS STRING) AdGroupName,
  SAFE_CAST(conversions AS STRING) Conversions,
  SAFE_CAST(A.date AS DATE) TimePeriod,
  SAFE_CAST(account_name AS STRING) AccountName,
  SAFE_CAST(A.ad_group_id AS STRING) AdGroupId,
  SAFE_CAST(clicks AS STRING) Clicks,
  SAFE_CAST(impressions AS STRING) Impressions,
  SAFE_CAST(campaign_name AS STRING) CampaignName,
  SAFE_CAST(revenue AS STRING) Revenue,
  SAFE_CAST(spend AS STRING) Spend,
  SAFE_CAST(A.account_id AS STRING) AccountId,
  SAFE_CAST(keyword_name AS STRING) Keyword,
  SAFE_CAST(A.keyword_id AS STRING) KeywordId,
  SAFE_CAST(ad_group_status AS STRING) AdGroupStatus,
  SAFE_CAST(bid_strategy_type AS STRING) BidStrategyType,
  SAFE_CAST(all_conversions AS STRING) AllConversions,
  SAFE_CAST(TRIM(currency_code) AS STRING) CurrencyCode,
  SAFE_CAST(final_url AS STRING) FinalUrl,
  SAFE_CAST(NULL AS STRING) FinalUrlSuffix,
  SAFE_CAST(NULL AS STRING) KeywordLabels,
  SAFE_CAST(network AS STRING) Network,
  SAFE_CAST(view_through_conversions AS STRING) ViewThroughConversions,
  SAFE_CAST(account_number AS STRING) AccountNumber,
  SAFE_CAST(all_revenue AS STRING) AllRevenue,
  SAFE_CAST(keyword_status AS STRING) KeywordStatus,
  SAFE_CAST(quality_score AS STRING) QualityScore,
  SAFE_CAST(average_position AS STRING) AveragePosition,
  SAFE_CAST(goal AS STRING) Goal,
FROM
  {{ source('microsoft', 'keyword_performance_daily_report') }} A
LEFT JOIN (
  SELECT
    DISTINCT id keyword_id,
    name keyword_name,
    ad_group_id,
    final_url,
    bid_strategy_type
  FROM
    {{ source('microsoft', 'keyword_history') }} ) B
ON
  A.keyword_id=B.keyword_id
  AND A.ad_group_id=B.ad_group_id
LEFT JOIN (
  SELECT
    DISTINCT id ad_group_id,
    name ad_group_name,
    status ad_group_status
  FROM
    {{ source('microsoft', 'ad_group_history') }} ) C
ON
  A.ad_group_id=C.ad_group_id
LEFT JOIN (
  SELECT
    DISTINCT name account_name,
    id account_id,
    number account_number,
  FROM
    {{ source('microsoft', 'account_history') }} ) D
ON
  A.account_id=D.account_id
LEFT JOIN (
  SELECT
    DISTINCT id campaign_id,
    name campaign_name,
  FROM
    {{ source('microsoft', 'campaign_history') }} ) E
ON
  A.campaign_id=E.campaign_id
LEFT JOIN (
  SELECT
    DISTINCT date,
    account_id,
    ad_group_id,
    campaign_id,
    keyword_id,
    goal,
  FROM
    {{ source('microsoft', 'goals_and_funnels_daily_report') }} ) F
ON
  A.date=F.date
  AND A.account_id=F.account_id
  AND A.ad_group_id=F.ad_group_id
  AND A.campaign_id=F.campaign_id
  AND A.keyword_id=F.keyword_id
