{{ config(
    materialized='table',
    post_hook="call system$log('info', 'Trading data quality checks completed. Check quarantine tables for failed records.')"
) }}

with raw_trading_data as (
    select * from {{ ref('stg_trading_books') }}
),

-- Define comprehensive data quality test conditions
validated_trading_data as (
    {{
        quarantine_failed_records(
            'raw_trading_data',
            [
                {
                    'name': 'valid_trade_id',
                    'test': 'trade_id is not null and length(trim(trade_id)) > 0'
                },
                {
                    'name': 'valid_trade_date',
                    'test': 'trade_date is not null and trade_date <= current_date() and trade_date >= date(\'2020-01-01\')'
                },
                {
                    'name': 'valid_trader_name',
                    'test': 'trader_name is not null and length(trim(trader_name)) > 0'
                },
                {
                    'name': 'valid_desk',
                    'test': 'desk is not null and desk in (\'Equity Trading\', \'FX Trading\', \'Fixed Income\', \'Derivatives\')'
                },
                {
                    'name': 'valid_ticker',
                    'test': 'ticker is not null and length(trim(ticker)) > 0'
                },
                {
                    'name': 'valid_quantity',
                    'test': 'quantity is not null and quantity > 0 and quantity <= 10000000'
                },
                {
                    'name': 'valid_price',
                    'test': 'price is not null and price > 0 and price <= 50000'
                },
                {
                    'name': 'valid_trade_type',
                    'test': 'trade_type is not null and upper(trade_type) in (\'BUY\', \'SELL\')'
                },
                {
                    'name': 'reasonable_trade_value',
                    'test': '(quantity * price) <= 100000000'
                },
                {
                    'name': 'valid_ticker_format',
                    'test': 'case when desk = \'FX Trading\' then ticker like \'%/%\' and length(ticker) = 7 else ticker regexp \'[A-Z]{1,5}\' end'
                }
            ],
            'trade_id'
        )
    }}
)

select 
    *,
    -- Add data quality flags
    'PASSED_ALL_TESTS' as data_quality_status,
    current_timestamp() as quality_check_timestamp,
    '{{ invocation_id }}' as quality_check_run_id
from validated_trading_data 