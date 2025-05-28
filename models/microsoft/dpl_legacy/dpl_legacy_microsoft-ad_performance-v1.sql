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
SAFE_CAST(Spend AS FLOAT64)	cost,			
SAFE_CAST(AccountId AS STRING)	account_id,			
SAFE_CAST(AdTitle AS STRING)	ad_title,			
SAFE_CAST(AdId AS STRING)	ad_id,			
SAFE_CAST(AllConversions AS FLOAT64)	all_conversions,			
SAFE_CAST(CustomerId AS STRING)	customer_id,			
SAFE_CAST(CustomerName AS STRING)	customer_name,			
SAFE_CAST(Goal AS STRING)	goal,			
SAFE_CAST(GoalType AS STRING)	goal_type,			
SAFE_CAST(TRIM(CurrencyCode) AS STRING)	currency,			
SAFE_CAST(Revenue AS FLOAT64)	revenue,			
SAFE_CAST(AveragePosition AS FLOAT64)	average_position,	
"microsoft-ad_performance-v1" AS raw_origin,
(SAFE_CAST(Spend AS FLOAT64) / source_b.ex_rate) _gbp_cost ,
(SAFE_CAST(Revenue AS FLOAT64) / source_b.ex_rate) _gbp_revenue , -- using AllRevenue column,

{{add_fields("CampaignName")}} /* Replace with the report's campaign name field */

FROM
 {{ref('microsoft-ad_performance-v1')}} source_a
left join exchange_rates as source_b
ON

SAFE_CAST(TimePeriod AS DATE) = source_b.day -- source_a.{datecolumn}

/* Jinja var if default field has null value..Replace the default field based on the report */

AND LOWER(IFNULL(TRIM(CurrencyCode),'{{var('account_currency')}}')) = source_b.currency_code -- Use this if the report already has a currency column. using TRIM to get rid of trailing whitespace
