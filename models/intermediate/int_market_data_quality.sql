{{ config(
    materialized='table',
    post_hook="call system$log('info', 'Market data quality checks completed. Check quarantine tables for failed records.')"
) }}

with stock_data as (
    select 
        *,
        'STOCK' as data_source
    from {{ ref('stg_stock_metrics') }}
),

forex_data as (
    select 
        run_date,
        currency_pair_name as ticker,
        open,
        high,
        low,
        close,
        null as volume,
        'FOREX' as data_source
    from {{ ref('stg_forex_metrics') }}
),

combined_market_data as (
    select * from stock_data
    union all
    select * from forex_data
),

-- Apply comprehensive OHLC and market data validation
validated_market_data as (
    {{
        quarantine_failed_records(
            'combined_market_data',
            [
                {
                    'name': 'valid_run_date',
                    'test': 'run_date is not null and run_date <= current_date() and run_date >= date(\'2020-01-01\')'
                },
                {
                    'name': 'valid_ticker_symbol',
                    'test': 'ticker is not null and length(trim(ticker)) > 0'
                },
                {
                    'name': 'valid_ohlc_not_null',
                    'test': 'open is not null and high is not null and low is not null and close is not null'
                },
                {
                    'name': 'valid_ohlc_positive',
                    'test': 'open > 0 and high > 0 and low > 0 and close > 0'
                },
                {
                    'name': 'valid_high_low_relationship',
                    'test': 'high >= low'
                },
                {
                    'name': 'valid_ohlc_range',
                    'test': 'open >= low and open <= high and close >= low and close <= high'
                },
                {
                    'name': 'reasonable_price_range',
                    'test': 'case when data_source = \'STOCK\' then high <= 10000 and low >= 0.01 when data_source = \'FOREX\' then high <= 10 and low >= 0.001 else true end'
                },
                {
                    'name': 'valid_daily_price_volatility',
                    'test': '((high - low) / ((high + low) / 2)) <= 0.5'
                },
                {
                    'name': 'valid_volume_stock',
                    'test': 'case when data_source = \'STOCK\' then (volume is null or volume >= 0) else true end'
                },
                {
                    'name': 'valid_forex_pair_format',
                    'test': 'case when data_source = \'FOREX\' then ticker like \'%/%\' and length(ticker) = 7 else true end'
                }
            ],
            'ticker'
        )
    }}
)

select 
    *,
    -- Add quality metrics
    'PASSED_ALL_TESTS' as data_quality_status,
    ((high - low) / ((high + low) / 2)) * 100 as daily_volatility_pct,
    ((close - open) / open) * 100 as daily_return_pct,
    current_timestamp() as quality_check_timestamp,
    '{{ invocation_id }}' as quality_check_run_id
from validated_market_data 