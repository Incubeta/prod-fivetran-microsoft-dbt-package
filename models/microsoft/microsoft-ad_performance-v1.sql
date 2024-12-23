{{
  custom_config(
    alias=var('microsoft_ad_performance_v1_alias','microsoft-ad_performance-v1'),
    field="TimePeriod")
}}

WITH
  ad_performance_daily_report AS (
  SELECT DISTINCT
    date,
    ad_id,
    account_id,
    campaign_id,
    ad_group_id,
    device_type,
    spend,
    clicks,
    impressions,
    conversions,
    all_conversions,
    currency_code,
    revenue,
    average_position
  FROM
    {{ source('microsoft', 'ad_performance_daily_report') }} ),

  ad_history AS (
  SELECT
    DISTINCT id ad_id,
    ad_group_id,
    title ad_title,
  FROM
    {{ source('microsoft', 'ad_history') }} ),

  ad_group_history AS (
  SELECT
    DISTINCT id ad_group_id,
    campaign_id,
    name ad_group_name,
  FROM
    {{ source('microsoft', 'ad_group_history') }} ),

  campaign_history AS (
  SELECT
    DISTINCT id campaign_id,
    account_id,
    name campaign_name,
    type campaign_type,
  FROM
    {{ source('microsoft', 'campaign_history') }} ),

  account_history AS (
  SELECT DISTINCT
    name account_name,
    id account_id,
    parent_customer_id customer_id,
    business_address_country_code country_code,
  FROM
    {{ source('microsoft', 'account_history') }} ),

  goals_and_funnels_daily_report AS (
  SELECT
    DISTINCT SAFE_CAST(device_type AS STRING) DeviceType,
    SAFE_CAST(campaign_id AS STRING) CampaignId,
    SAFE_CAST(ad_group_name AS STRING) AdGroupName,
    SAFE_CAST(0 AS STRING) Conversions,
    SAFE_CAST(date AS DATE) TimePeriod,
    SAFE_CAST(account_name AS STRING) AccountName,
    SAFE_CAST(ad_group_id AS STRING) AdGroupId,
    SAFE_CAST(0 AS STRING) Clicks,
    SAFE_CAST(0 AS STRING) Impressions,
    SAFE_CAST(campaign_name AS STRING) CampaignName,
    SAFE_CAST(0 AS STRING) Spend,
    SAFE_CAST(account_id AS STRING) AccountId,
    SAFE_CAST(NULL AS STRING) AdTitle,
    SAFE_CAST(NULL AS STRING) AdId,
    SAFE_CAST(all_conversions AS STRING) AllConversions,
    SAFE_CAST(customer_id AS STRING) CustomerId,
    SAFE_CAST(account_name AS STRING) CustomerName,
    SAFE_CAST(goal AS STRING) Goal,
    SAFE_CAST(goal_type AS STRING) GoalType,
    SAFE_CAST(country_code AS STRING) CurrencyCode,
    SAFE_CAST(all_revenue AS STRING) Revenue,
    SAFE_CAST(0 AS STRING) AveragePosition,
    SAFE_CAST(campaign_type AS STRING) CampaignType,
  FROM
    {{ source('microsoft', 'goals_and_funnels_daily_report') }}
  LEFT JOIN
    account_history
  USING
    (account_id)
  LEFT JOIN
    ad_group_history
  USING
    (ad_group_id,
      campaign_id)
  LEFT JOIN
    campaign_history
  USING
    (account_id,
      campaign_id)),

  ad_performance_without_goals AS (
  SELECT
    DISTINCT SAFE_CAST(device_type AS STRING) DeviceType,
    SAFE_CAST(campaign_id AS STRING) CampaignId,
    SAFE_CAST(ad_group_name AS STRING) AdGroupName,
    SAFE_CAST(conversions AS STRING) Conversions,
    SAFE_CAST(date AS DATE) TimePeriod,
    SAFE_CAST(account_name AS STRING) AccountName,
    SAFE_CAST(ad_group_id AS STRING) AdGroupId,
    SAFE_CAST(clicks AS STRING) Clicks,
    SAFE_CAST(impressions AS STRING) Impressions,
    SAFE_CAST(campaign_name AS STRING) CampaignName,
    SAFE_CAST(spend AS STRING) Spend,
    SAFE_CAST(account_id AS STRING) AccountId,
    SAFE_CAST(ad_title AS STRING) AdTitle,
    SAFE_CAST(ad_id AS STRING) AdId,
    SAFE_CAST(all_conversions AS STRING) AllConversions,
    SAFE_CAST(customer_id AS STRING) CustomerId,
    SAFE_CAST(NULL AS STRING) CustomerName,
    SAFE_CAST(NULL AS STRING) Goal,
    SAFE_CAST(NULL AS STRING) GoalType,
    SAFE_CAST(TRIM(currency_code) AS STRING) CurrencyCode,
    SAFE_CAST(revenue AS STRING) Revenue,
    SAFE_CAST(average_position AS STRING) AveragePosition,
    SAFE_CAST(campaign_type AS STRING) CampaignType,
  FROM
    ad_performance_daily_report
  LEFT JOIN
    ad_history
  USING
    (ad_id,
      ad_group_id)
  LEFT JOIN
    ad_group_history
  USING
    (ad_group_id,
      campaign_id)
  LEFT JOIN
    campaign_history
  USING
    (account_id,
      campaign_id)
  LEFT JOIN
    account_history
  USING
    (account_id))

SELECT
  *
FROM
  ad_performance_without_goals
UNION ALL
SELECT
  *
FROM
  goals_and_funnels_daily_report
