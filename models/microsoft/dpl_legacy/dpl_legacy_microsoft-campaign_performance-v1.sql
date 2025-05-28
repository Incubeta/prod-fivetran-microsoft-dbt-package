with exchange_rates as (
  select *
    from {{ref('stg_openexchange_rates__openexchange_report_v1')}}
  )
  SELECT 
SAFE_CAST(DeviceType	AS STRING )	device_type ,			
SAFE_CAST(CampaignId	AS STRING )	campaign_id ,			
SAFE_CAST(Conversions	AS INT64 )	conversions ,
SAFE_CAST(TimePeriod	AS DATE )	day ,			
SAFE_CAST(AccountName	AS STRING )	account_name ,			
SAFE_CAST(Clicks	AS INT64 )	clicks ,			
SAFE_CAST(Impressions	AS INT64 )	impressions ,			
SAFE_CAST(Revenue	AS FLOAT64 )	revenue ,			
SAFE_CAST(CampaignName	AS STRING )	campaign_name ,			
SAFE_CAST(Spend	AS FLOAT64 )	cost ,			
SAFE_CAST(AccountId	AS STRING )	account_id ,			
SAFE_CAST(AccountNumber	AS STRING )	account_number ,			
SAFE_CAST(AdDistribution	AS STRING )	ad_distribution ,			
SAFE_CAST(AdRelevance	AS INT64 )	ad_relevance ,			
SAFE_CAST(AllConversions	AS INT64 )	all_conversions ,			
SAFE_CAST(AllRevenue	AS FLOAT64 )	all_revenue ,			
SAFE_CAST(AveragePosition	AS FLOAT64 )	average_position ,			
SAFE_CAST(CampaignLabels	AS STRING )	campaign_labels ,			
SAFE_CAST(CampaignStatus	AS STRING )	campaign_status ,			
SAFE_CAST(CampaignType	AS STRING )	campaign_type ,			
SAFE_CAST(TRIM(CurrencyCode)	AS STRING )	currency ,	--using TRIM to get rid of trailing whitespace		
SAFE_CAST(CustomerId	AS STRING )	customer_id ,			
SAFE_CAST(CustomerName	AS STRING )	customer_name ,			
SAFE_CAST(CustomParameters	AS STRING )	custom_parameters ,			
SAFE_CAST(FinalUrlSuffix	AS STRING )	final_url_suffix ,			
SAFE_CAST(HistoricalQualityScore	AS INT64 )	historical_quality_score ,			
SAFE_CAST(LandingPageExperience	AS INT64 )	landing_page_experience ,			
SAFE_CAST(LowQualityClicks	AS INT64 )	low_quality_clicks ,			
SAFE_CAST(LowQualityConversions	AS INT64 )	low_quality_conversions ,			
SAFE_CAST(LowQualityGeneralClicks	AS INT64 )	low_quality_general_clicks ,			
SAFE_CAST(LowQualityImpressions	AS INT64 )	low_quality_impressions ,			
SAFE_CAST(Network	AS STRING )	network ,			
SAFE_CAST(PhoneCalls	AS INT64 )	phone_calls ,			
SAFE_CAST(PhoneImpressions	AS INT64 )	phone_impressions ,			
SAFE_CAST(QualityScore	AS INT64 )	quality_score ,			
SAFE_CAST(TrackingTemplate	AS STRING )	tracking_template ,			
SAFE_CAST(ViewThroughConversions	AS INT64 )	view_through_conversions ,			
SAFE_CAST(ClickSharePercent	AS FLOAT64 )	click_share_percent ,			
SAFE_CAST(ImpressionSharePercent	AS INT64 )	impression_share_percent ,			
SAFE_CAST(TopImpressionSharePercent	AS FLOAT64 )	top_impression_share_percent ,			
SAFE_CAST(AbsoluteTopImpressionSharePercent	AS FLOAT64 )	absolute_top_impression_share_percent ,
"microsoft-campaign_performance-v1" AS raw_origin,
(SAFE_CAST(Spend AS FLOAT64) / source_b.ex_rate) _gbp_cost ,
(SAFE_CAST(AllRevenue AS FLOAT64) / source_b.ex_rate) _gbp_revenue , -- using AllRevenue column,

{{add_fields("CampaignName")}} /* Replace with the report's campaign name field */

FROM
 {{ref('microsoft-campaign_performance-v1')}} source_a
left join exchange_rates as source_b
ON

SAFE_CAST(TimePeriod AS DATE) = source_b.day -- source_a.{datecolumn}

/* Jinja var if default field has null value..Replace the default field based on the report */

AND LOWER(IFNULL(TRIM(CurrencyCode),'{{var('account_currency')}}')) = source_b.currency_code -- Use this if the report already has a currency column. using TRIM to get rid of trailing whitespace
