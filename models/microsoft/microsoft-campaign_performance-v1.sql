{{
  custom_config(
    alias=var('microsoft_campaign_performance_v1_alias','microsoft-campaign_performance-v1'),
    field="TimePeriod")
}}

WITH
  campaign_perf_report AS (
  SELECT DISTINCT
    device_type,
    campaign_id,
    campaign_name,
    campaign_status,
    currency_code,
    account_id,
    ad_distribution,
    custom_parameters,
    network,
    date,
    SUM(clicks) clicks,
    SUM(conversions) conversions,
    SUM(impressions) impressions,
    SUM(revenue) revenue,
    SUM(spend) spend,
    SUM(ad_relevance) ad_relevance,
    SUM(all_conversions) all_conversions,
    SUM(all_revenue) all_revenue,
    SUM(average_position) average_position,
    SUM(historical_quality_score) historical_quality_score,
    SUM(landing_page_experience) landing_page_experience,
    SUM(low_quality_clicks) low_quality_clicks,
    SUM(low_quality_conversions) low_quality_conversions,
    SUM(low_quality_general_clicks) low_quality_general_clicks,
    SUM(low_quality_impressions) low_quality_impressions,
    SUM(phone_calls) phone_calls,
    SUM(phone_impressions) phone_impressions,
    SUM(quality_score) quality_score,
    SUM(view_through_conversions) view_through_conversions,
  FROM
    {{ source('microsoft', 'campaign_performance_daily_report') }}
  GROUP BY 1,2,3,4,5,6,7,8,9,10)

SELECT DISTINCT
    SAFE_CAST(A.device_type AS STRING ) DeviceType,
    SAFE_CAST(A.campaign_id AS STRING ) CampaignId,
    SAFE_CAST(conversions AS STRING ) Conversions,
    SAFE_CAST(A.date AS DATE ) TimePeriod,
    SAFE_CAST(account_name AS STRING ) AccountName,
    SAFE_CAST(clicks AS STRING ) Clicks,
    SAFE_CAST(impressions AS STRING ) Impressions,
    SAFE_CAST(revenue AS STRING ) Revenue,
    SAFE_CAST(campaign_name AS STRING ) CampaignName,
    SAFE_CAST(spend AS STRING ) Spend,
    SAFE_CAST(A.account_id AS STRING ) AccountId,
    SAFE_CAST(account_number AS STRING ) AccountNumber,
    SAFE_CAST(ad_distribution AS STRING ) AdDistribution,
    SAFE_CAST(ad_relevance AS STRING ) AdRelevance,
    SAFE_CAST(all_conversions AS STRING ) AllConversions,
    SAFE_CAST(all_revenue AS STRING ) AllRevenue,
    SAFE_CAST(average_position AS STRING ) AveragePosition,
    SAFE_CAST(NULL AS STRING ) CampaignLabels,
    SAFE_CAST(campaign_status AS STRING ) CampaignStatus,
    SAFE_CAST(type AS STRING ) CampaignType,
    SAFE_CAST(TRIM(A.currency_code) AS STRING ) CurrencyCode,
    SAFE_CAST(B.customer_id AS STRING ) CustomerId,
    SAFE_CAST(E.customer_name AS STRING ) CustomerName,
    SAFE_CAST(custom_parameters AS STRING ) CustomParameters,
    SAFE_CAST(NULL AS STRING ) FinalUrlSuffix,
    SAFE_CAST(historical_quality_score AS STRING ) HistoricalQualityScore,
    SAFE_CAST(landing_page_experience AS STRING ) LandingPageExperience,
    SAFE_CAST(low_quality_clicks AS STRING ) LowQualityClicks,
    SAFE_CAST(low_quality_conversions AS STRING ) LowQualityConversions,
    SAFE_CAST(low_quality_general_clicks AS STRING ) LowQualityGeneralClicks,
    SAFE_CAST(low_quality_impressions AS STRING ) LowQualityImpressions,
    SAFE_CAST(A.network AS STRING ) Network,
    SAFE_CAST(phone_calls AS STRING ) PhoneCalls,
    SAFE_CAST(phone_impressions AS STRING ) PhoneImpressions,
    SAFE_CAST(quality_score AS STRING ) QualityScore,
    SAFE_CAST(NULL AS STRING ) TrackingTemplate,
    SAFE_CAST(view_through_conversions AS STRING ) ViewThroughConversions,
    SAFE_CAST(click_share_percent AS STRING ) ClickSharePercent,
    SAFE_CAST(impression_share_percent AS STRING ) ImpressionSharePercent,
    SAFE_CAST(top_impression_share_percent AS STRING ) TopImpressionSharePercent,
    SAFE_CAST(absolute_top_impression_share_percent AS STRING ) AbsoluteTopImpressionSharePercent,
FROM
     campaign_perf_report A
LEFT JOIN (
  SELECT DISTINCT
    name account_name,
    id,
    number account_number,
    parent_customer_id customer_id
  FROM
    {{ source('microsoft', 'account_history') }}) B
  ON
    A.account_id=B.id
LEFT JOIN (
  SELECT DISTINCT
    id,
    type,
  FROM
    {{ source('microsoft', 'campaign_history') }}
  where type is not null) C
  ON
    A.campaign_id=C.id
LEFT JOIN (
  SELECT DISTINCT
    date,
    account_id,
    campaign_id,
    device_type,
    network,
    currency_code,
    SUM(click_share_percent) click_share_percent,
    SUM(impression_share_percent) impression_share_percent,
    SUM(absolute_top_impression_share_percent) absolute_top_impression_share_percent,
    SUM(top_impression_share_percent) top_impression_share_percent,
  FROM
    {{ source('microsoft', 'campaign_impression_performance_daily_report') }}
    GROUP BY 1,2,3,4,5,6) D
  ON
    A.campaign_id=D.campaign_id
    AND A.account_id=D.account_id
    AND A.date=D.date
    AND A.device_type=D.device_type
    AND A.network=D.network
    AND A.currency_code=D.currency_code
LEFT JOIN (
  SELECT DISTINCT
    customer_id,
    customer_name
  FROM
      {{ source('microsoft', 'destination_url_performance_daily_report') }}) E
  ON
    B.customer_id=E.customer_id
