{{
  config(
    materialized='table',
    post_hook="{{ log('Trading data quality check completed. Check quarantine table for failed records.', info=True) }}"
  )
}}

{# Define data quality test conditions #}
{% set test_conditions = [
    {
        'name': 'quantity_positive',
        'test': 'quantity > 0'
    },
    {
        'name': 'price_positive', 
        'test': 'price > 0'
    },
    {
        'name': 'valid_desk',
        'test': 'desk in (\'EQUITIES\', \'FIXED_INCOME\', \'FX\', \'COMMODITIES\', \'DERIVATIVES\')'
    },
    {
        'name': 'valid_trade_type',
        'test': 'trade_type in (\'BUY\', \'SELL\')'
    },
    {
        'name': 'trade_date_not_future',
        'test': 'trade_date <= current_date()'
    }
] %}

with source_data as (
    select 
        trade_id,
        trade_date,
        trader_name,
        desk,
        ticker,
        quantity,
        price,
        trade_type,
        notes,
        current_timestamp() as processed_at
    from {{ ref('stg_trading_books') }}
),

{# Use quarantine handler to separate clean vs failed records #}
quality_checked as (
    {{ quarantine_failed_records('source_data', test_conditions, 'trade_id') }}
)

select 
    *,
    'PASSED_ALL_TESTS' as data_quality_status,
    quantity * price as trade_value
from quality_checked 