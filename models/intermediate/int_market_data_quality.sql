{{
  config(
    materialized='table',
    post_hook="{{ log('Market data quality check completed. Check quarantine table for failed records.', info=True) }}"
  )
}}

{# Define data quality test conditions for market data #}
{% set test_conditions = [
    {
        'name': 'high_gte_open',
        'test': 'high >= open_price'
    },
    {
        'name': 'high_gte_low',
        'test': 'high >= low_price'
    },
    {
        'name': 'high_gte_close',
        'test': 'high >= close_price'
    },
    {
        'name': 'low_lte_open',
        'test': 'low_price <= open_price'
    },
    {
        'name': 'low_lte_high',
        'test': 'low_price <= high'
    },
    {
        'name': 'low_lte_close',
        'test': 'low_price <= close_price'
    },
    {
        'name': 'all_prices_positive',
        'test': 'open_price > 0 and high > 0 and low_price > 0 and close_price > 0'
    },
    {
        'name': 'volume_non_negative',
        'test': 'coalesce(volume, 0) >= 0'
    },
    {
        'name': 'reasonable_volatility',
        'test': '((high - low_price) / open_price * 100) <= 50'
    }
] %}

with combined_market_data as (
    -- Combine stock and forex data
    select 
        run_date,
        ticker as symbol,
        'STOCK' as data_type,
        open_price,
        high_price as high,
        low_price,
        close_price,
        volume,
        current_timestamp() as processed_at
    from {{ ref('stg_stock_metrics') }}
    
    union all
    
    select 
        run_date,
        currency_pair_name as symbol,
        'FOREX' as data_type,
        open_rate as open_price,
        high_rate as high,
        low_rate as low_price,
        close_rate as close_price,
        null as volume,
        current_timestamp() as processed_at
    from {{ ref('stg_forex_metrics') }}
),

{# Use quarantine handler to separate clean vs failed records #}
quality_checked as (
    {{ quarantine_failed_records('combined_market_data', test_conditions, 'symbol || run_date') }}
)

select 
    *,
    'PASSED_ALL_TESTS' as data_quality_status,
    ((high - low_price) / open_price * 100) as daily_volatility_pct
from quality_checked 