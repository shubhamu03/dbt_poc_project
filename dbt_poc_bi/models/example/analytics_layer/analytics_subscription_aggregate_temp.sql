-----------------analytics_subscription_aggregates ---------------
{{ config(
  materialized='incremental',
  unique_key='unique_key',
  file_format='delta',
  partition_by=['report_date'],
  tags=["semantic", "analytics", "daily","subscription"]
) }}
------------------ Get valid records for the selected time range from the subscriptions daily snapshot table----------------------------
WITH sub_source as (
    SELECT
        daily_snapshot.report_date,
        case 
            when daily_snapshot.provider = 'iap' then daily_snapshot.provider_type
            when daily_snapshot.provider = 'direct' then daily_snapshot.payment_provider
            when daily_snapshot.provider = 'partner' then daily_snapshot.provider_type
            else daily_snapshot.provider_type
        end payment_provider,
        case when daily_snapshot.tier_type IS NULL  THEN ad_tier
            ELSE  LOWER(REPLACE(tier_type,'TIER_TYPE_',''))
        end as tier_type,
        daily_snapshot.affiliate,
        case when daily_snapshot.subs_paid_ind = 1 then 'Y' else 'N' end paid,
        CASE
            WHEN daily_snapshot.has_started_with_free_trial = 1
            THEN 'Y'
            ELSE 'N'
        END AS subs_started_free_trial,
        daily_snapshot.ad_tier,
        daily_snapshot.provider AS source,
        daily_snapshot.provider_type,
        coalesce(daily_snapshot.migration_type, 'New Max User') AS migration_type,
        daily_snapshot.market,
        daily_snapshot.realm,
        CASE
            WHEN daily_snapshot.subscription_payment_period_type IS NOT NULL
            THEN substr (daily_snapshot.subscription_payment_period_type,8)
            ELSE daily_snapshot.subscription_payment_period_type
        END AS payment_period,
        daily_snapshot.campaign_name AS campaign,
        daily_snapshot.region,
        daily_snapshot.territory,
        daily_snapshot.product_name,
        daily_snapshot.subs_termination_code AS termination_code,
        daily_snapshot.subs_purchase_type,
        daily_snapshot.on_grace_period,
        daily_snapshot.closing_active_ind,
        daily_snapshot.gross_add_ind,
        daily_snapshot.new_acquisition_ind,
        daily_snapshot.winback_ind,
        daily_snapshot.canceled_ind,
        daily_snapshot.intraday_transfer_ind,
        daily_snapshot.ended_intraday_transfer_ind,
        daily_snapshot.churn_ind,
        daily_snapshot.paused_ind,
        daily_snapshot.resumed_ind,
        daily_snapshot.closing_paused_ind,
        daily_snapshot.user_id,
        daily_snapshot.subscription_status
    FROM {{ source('subscriptions', 'drv_subscriptions_daily_snapshot') }} daily_snapshot
    WHERE
        subs_valid_ind = 1
        AND {{ incremental_filter('daily_snapshot.report_date','day') }}
),
subs_data AS (
    select
        *,
    {{generate_unique_key([
        'payment_provider'
        ,'tier_type'
        ,'affiliate'
        ,'paid'
        ,'subs_started_free_trial'
        ,'ad_tier'
        ,'source'
        ,'provider_type'
        ,'migration_type'
        ,'market'
        ,'realm'
        ,'payment_period'
        ,'campaign'
        ,'region'
        ,'territory'
        ,'product_name'
        ,'subs_purchase_type'
    ])}} as  agg_key
    from
        sub_source
), daily_rollup as (
    SELECT
    {{generate_unique_key([
        "'day'"
        ,'string(report_date)'
        ,'payment_provider'
        ,'tier_type'
        ,'affiliate'
        ,'paid'
        ,'subs_started_free_trial'
        ,'ad_tier'
        ,'source'
        ,'provider_type'
        ,'migration_type'
        ,'market'
        ,'realm'
        ,'payment_period'
        ,'campaign'
        ,'region'
        ,'territory'
        ,'product_name'
        ,'subs_purchase_type'
    ])}} as unique_key,
        'day' AS grain,
        agg_key,
        report_date,
        payment_provider,
        tier_type,
        affiliate,
        paid,
        subs_started_free_trial,
        ad_tier,
        source,
        provider_type,
        migration_type,
        market,
        realm,
        payment_period,
        campaign,
        region,
        territory,
        product_name,
        subs_purchase_type,
        COUNT(DISTINCT (CASE WHEN closing_active_ind = 1 THEN user_id ELSE NULL END)) AS closing_active_subs,
        COUNT(DISTINCT (CASE WHEN lower(subs_purchase_type) in ('upgrade', 'downgrade')  THEN NULL
                                WHEN gross_add_ind = 1 THEN user_id ELSE NULL END)) AS gross_adds,
        COUNT(DISTINCT (CASE WHEN lower(subs_purchase_type) in ('upgrade', 'downgrade')  and gross_add_ind = 1  THEN user_id ELSE NULL END)) AS transfers_in,
        COUNT(DISTINCT (CASE WHEN new_acquisition_ind = 1 THEN user_id ELSE NULL END)) AS new_sale,
        COUNT(DISTINCT (CASE WHEN winback_ind = 1 THEN user_id ELSE NULL END)) AS winback,
        COUNT(DISTINCT (CASE WHEN canceled_ind = 1 THEN user_id ELSE NULL END)) AS cancelled_subs,
        COUNT(DISTINCT (CASE WHEN intraday_transfer_ind = 1 THEN user_id ELSE NULL END)) AS intraday_transfers,
        COUNT(DISTINCT (CASE WHEN ended_intraday_transfer_ind = 1 THEN user_id ELSE NULL END)) AS ended_intraday_transfers,
        COUNT(DISTINCT (CASE WHEN churn_ind = 1 THEN user_id ELSE NULL END)) AS new_churn,
        COUNT(DISTINCT (CASE WHEN churn_ind = 1 AND upper(termination_code) 
                IN ('TERMINATION_CODE_USER_DOWNGRADE','TERMINATION_CODE_USER_UPGRADE',
                'TERMINATION_CODE_USER_UP_OR_DOWNGRADE','TERMINATION_CODE_CANCELED_BY_USER',
                'TERMINATION_CODE_CANCELED_ON_PARTNER_SWITCH'
                 ) THEN user_id ELSE NULL END)) AS voluntary_churn,
        COUNT(DISTINCT (CASE WHEN churn_ind = 1 AND (upper(termination_code) NOT 
                IN ('TERMINATION_CODE_USER_DOWNGRADE','TERMINATION_CODE_USER_UPGRADE',
                'TERMINATION_CODE_USER_UP_OR_DOWNGRADE','TERMINATION_CODE_CANCELED_BY_USER',
                'TERMINATION_CODE_CANCELED_ON_PARTNER_SWITCH')
                or termination_code is null) THEN user_id ELSE NULL END)) AS involuntary_churn,
        COUNT(DISTINCT (CASE WHEN paused_ind = 1 THEN user_id ELSE NULL END)) AS new_paused,
        COUNT(DISTINCT (CASE WHEN resumed_ind = 1 THEN user_id ELSE NULL END)) AS new_resumed,
        COUNT(DISTINCT (CASE WHEN closing_paused_ind = 1 THEN user_id ELSE NULL END)) AS closing_paused_subs,
        COUNT(DISTINCT (CASE WHEN on_grace_period = 1 and subscription_status in ('STATUS_ACTIVE', 'STATUS_CANCELED') THEN user_id ELSE NULL END)) AS subs_in_grace_period
    FROM subs_data s
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21
),

prev_day AS (
    
{%- if is_incremental() %}
select
    prev_day.unique_key,
    {{generate_unique_key([
        'payment_provider'
        ,'tier_type'
        ,'affiliate'
        ,'paid'
        ,'subs_started_free_trial'
        ,'ad_tier'
        ,'source'
        ,'provider_type'
        ,'migration_type'
        ,'market'
        ,'realm'
        ,'payment_period'
        ,'campaign'
        ,'region'
        ,'territory'
        ,'product_name'
        ,'subs_purchase_type'
    ])}} as  agg_key,  agg_key,
    max(prev_day.REPORT_DATE) REPORT_DATE,
    sum(prev_day.closing_active_subs) closing_active_subs,
    sum(prev_day.gross_adds) gross_adds
    from {{ source('subscriptions', 'analytics_subscription_aggregates_daily') }} prev_day
    where  prev_day.grain = 'day'
    and date(prev_day.report_date) =(select max(report_date) report_date from {{ source('subscriptions', 'analytics_subscription_aggregates_daily') }} )  ----previous report date
            -- date_add(DATE('{{ generate_working_timestamp(var("start_timestamp"),var("end_timestamp")) }}'),-2) 
    group by 1,2
{%- else -%}
    -- if it's not incremental just null out the fields
    -- TODO: is it better to cast the correct types here?
    select 
        '' as unique_key,
        '' as agg_key,
        '' as REPORT_DATE,
        '' as closing_active_subs,
        '' as gross_adds    
{%- endif %}

),curr_day as
(
    SELECT
    unique_key,
    agg_key,
    REPORT_DATE
    from daily_rollup
    group by 1,2,3 

),missing_unique_keys AS
(
    select DISTINCT prev_day.unique_key 
    from curr_day FULL OUTER  join prev_day 
    on prev_day.agg_key = curr_day.agg_key
    WHERE curr_day.unique_key IS NULL
    AND prev_day.closing_active_subs > 0
)

 {%- if is_incremental() %}
select 
     
    {{generate_unique_key([
        "'day'"
        ,'report_date'
        ,'payment_provider'
        ,'tier_type'
        ,'affiliate'
        ,'paid'
        ,'subs_started_free_trial'
        ,'ad_tier'
        ,'source'
        ,'provider_type'
        ,'migration_type'
        ,'market'
        ,'realm'
        ,'payment_period'
        ,'campaign'
        ,'region'
        ,'territory'
        ,'product_name'
        ,'subs_purchase_type'
    ])}} as unique_key,

    *

from
    (select 

    grain,
    date_add(DATE('{{ generate_working_timestamp(var("start_timestamp"),var("end_timestamp")) }}'),-1) report_date,
    payment_provider,
    tier_type,
    affiliate,
    paid,
    subs_started_free_trial,
    ad_tier,
    source,
    provider_type,
    migration_type,
    market,
    realm,
    payment_period,
    campaign,
    region,
    territory,
    product_name,
    subs_purchase_type,
    0 closing_active_subs,
    0 gross_adds,
    0 transfers_in,
    0 new_sale,
    0 winback,
    0 cancelled_subs,
    0 intraday_transfers,
    0 ended_intraday_transfers,
    0 new_churn,
    0 voluntary_churn,
    0 involuntary_churn,
    0 new_paused,
    0 new_resumed,
    0 closing_paused_subs,
    0 subs_in_grace_period,
    gross_adds gross_adds_prev,
    closing_active_subs closing_active_subs_prev,
    {{ dbt_metadata_columns() }}

    from {{ source('subscriptions', 'analytics_subscription_aggregates_daily') }} 
    where  grain = 'day'
    and date(report_date) = (select max(report_date) report_date from {{ source('subscriptions', 'analytics_subscription_aggregates_daily') }} ) ----previous report date
    and unique_key in (select unique_key from missing_unique_keys )
    )
 {%- else -%}
    -- if it's not incremental just null out the fields
    -- TODO: is it better to cast the correct types here?
select 
'' as unique_key,
'' as grain,
'' as report_date,
'' as payment_provider,
'' as tier_type,
'' as affiliate,
'' as paid,
'' as subs_started_free_trial,
'' as ad_tier,
'' as source,
'' as provider_type,
'' as migration_type,
'' as market,
'' as realm,
'' as payment_period,
'' as campaign,
'' as region,
'' as territory,
'' as product_name,
'' as subs_purchase_type,
'' as closing_active_subs,
'' as gross_adds,
'' as transfers_in,
'' as new_sale,
'' as winback,
'' as cancelled_subs,
'' as intraday_transfers,
'' as ended_intraday_transfers,
'' as new_churn,
'' as voluntary_churn,
'' as involuntary_churn,
'' as new_paused,
'' as new_resumed,
'' as closing_paused_subs,
'' as subs_in_grace_period,
'' as gross_adds_prev,
'' as closing_active_subs_prev,
{{ dbt_metadata_columns() }}
 {%- endif %}

union all 

------------------ day level aggregate ------------------------------		
select 
    daily_rollup.unique_key,
    daily_rollup.grain,
    daily_rollup.report_date,
    daily_rollup.payment_provider,
    daily_rollup.tier_type,
    daily_rollup.affiliate,
    paid,
    daily_rollup.subs_started_free_trial,
    daily_rollup.ad_tier,
    daily_rollup.source,
    daily_rollup.provider_type,       
    daily_rollup.migration_type,
    daily_rollup.market,
    daily_rollup.realm,
    daily_rollup.payment_period,
    daily_rollup.campaign,
    daily_rollup.region,
    daily_rollup.territory,
    daily_rollup.product_name,
    daily_rollup.subs_purchase_type,
    daily_rollup.closing_active_subs,
    daily_rollup.gross_adds,
    daily_rollup.transfers_in,
    daily_rollup.new_sale,
    daily_rollup.winback,
    daily_rollup.cancelled_subs,
    daily_rollup.intraday_transfers,
    daily_rollup.ended_intraday_transfers,
    daily_rollup.new_churn,
    daily_rollup.voluntary_churn,
    daily_rollup.involuntary_churn,
    daily_rollup.new_paused,
    daily_rollup.new_resumed,
    daily_rollup.closing_paused_subs,
    daily_rollup.subs_in_grace_period,
    prev_day.gross_adds gross_adds_prev,
    prev_day.closing_active_subs closing_active_subs_prev,
    {{ dbt_metadata_columns() }}
from daily_rollup
left join prev_day on daily_rollup.agg_key = prev_day.agg_key