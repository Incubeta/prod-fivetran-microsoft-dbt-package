# Fivetran Microsoft Ads dbt package

## What does this dbt package do?
* Materializes the Microsoft Ads RAW_main tables using the data coming from the Microsoft API.

## How do I use the dbt package?
### Step 1: Prerequisites
To use this dby package, you must have the following:
- At least one Fivetran Microsoft connector syncing data for at least one of the predefined reports:
    - campaign_performance_daily_report
    - keyword_performance_daily_report
    - account_history
    - campaign_history
    - account_impression_performance_daily_report
- A BigQuery data destination

### Step 2: Install the package
Include the following microsoft package version in your `packages.yml` file

### Step 3: Define input tables variables
This package reads the microsoft/bings data from the different tables created by the microsoft ads connector. 
The names of the tables can be changed by setting the correct name in the root `dbt_project.yml` file.

The following table shows the configuration keys and the default table names:

|key|default|
|---|-------|
|campaign_performance_daily_report_identifier|campaign_performance_daily_report|
|keyword_performance_daily_report_identifier|keyword_performance_daily_report|
|account_history_identifier|account_history|
|campaign_history_identifier|campaign_history|
|account_impression_performance_daily_report_identifier|account_impression_performance_daily_report|


If the connector uses different table names (for example campaign_performance_daily_report) this can be set in the `dbt_project.yml` as follows.

```yaml
vars:
    campaign_performance_daily_report_identifier: campaign_performance_daily_report 
    account_history_identifier: account_history
    campaign_history_identifier: campaign_history
    account_impression_performance_daily_report_identifier: account_impression_performance_daily_report
```

### (Optional) Step 4: Additional configurations

#### Disable reports:
Individual reports can be disabled in the `dbt_project.yml` file.

```yaml
vars:
    
```

#### Change output tables:
The following vars can be used to change the output table names:

|key| default                           |
|---|-----------------------------------|
|campaign_performance_daily_report_alias| microsoft-campaign_performance-v1 |
|keyword_performance_daily_report_alias| microsoft-keyword_performance-v1  |


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
