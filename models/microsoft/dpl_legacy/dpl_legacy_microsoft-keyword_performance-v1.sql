with exchange_rates as (
  select *
    from {{ref('stg_openexchange_rates__openexchange_report_v1')}}
  )
SELECT
    SAFE_CAST(DeviceType AS STRING)	device_type,
    SAFE_CAST(CampaignId AS STRING)	campaign_id,
    SAFE_CAST(AdGroupName AS STRING)	ad_group_name,
    SAFE_CAST(Conversions AS INT64)	conversions,
    SAFE_CAST(TimePeriod AS DATE)	day,
    SAFE_CAST(AccountName AS STRING)	account_name,
    SAFE_CAST(AdGroupId AS STRING)	ad_group_id,
    SAFE_CAST(Clicks AS INT64)	clicks,
    SAFE_CAST(Impressions AS INT64)	impressions,
    SAFE_CAST(CampaignName AS STRING)	campaign_name,
    SAFE_CAST(Revenue AS FLOAT64)	revenue,
    SAFE_CAST(Spend AS FLOAT64)	cost,
    SAFE_CAST(AccountId AS STRING)	account_id,
    SAFE_CAST(Keyword AS STRING)	keyword,
    SAFE_CAST(KeywordId AS STRING)	keyword_id,
    SAFE_CAST(AdGroupStatus AS STRING)	ad_group_status,
    SAFE_CAST(BidStrategyType AS STRING)	bid_strategy_type,
    SAFE_CAST(AllConversions AS INT64)	all_conversions,
    SAFE_CAST(TRIM(CurrencyCode) AS STRING)	currency,
    SAFE_CAST(FinalUrl AS STRING)	final_url,
    SAFE_CAST(FinalUrlSuffix AS STRING)	final_url_suffix,
    SAFE_CAST(KeywordLabels AS STRING)	keyword_labels,
    SAFE_CAST(Network AS STRING)	network,
    SAFE_CAST(ViewThroughConversions AS INT64)	view_through_conversions,
    SAFE_CAST(AccountNumber AS STRING)	account_number,
    SAFE_CAST(AllRevenue AS FLOAT64)	all_revenue,
    SAFE_CAST(KeywordStatus AS STRING)	keyword_status,
    SAFE_CAST(QualityScore AS INT64)	quality_score,
    SAFE_CAST(AveragePosition AS FLOAT64)	average_position,
    SAFE_CAST(Goal AS STRING)	goal,
       'microsoft-keyword_performance-v1' AS raw_origin,
    (SAFE_CAST(Spend AS FLOAT64) / source_b.ex_rate) _gbp_cost ,
    (SAFE_CAST(AllRevenue AS FLOAT64) / source_b.ex_rate) _gbp_revenue , -- using AllRevenue column,

    {{add_fields("CampaignName")}} /* Replace with the report's campaign name field */
FROM
 {{ref('microsoft-keyword_performance-v1')}} source_a
left join exchange_rates as source_b
ON

SAFE_CAST(TimePeriod AS DATE) = source_b.day -- source_a.{datecolumn}

/* Jinja var if default field has null value..Replace the default field based on the report */

AND LOWER(IFNULL(TRIM(CurrencyCode),'{{var('account_currency')}}')) = source_b.currency_code -- Use this if the report already has a currency column. using TRIM to get rid of trailing whitespace
