-- Custom test to ensure critical trading data completeness
-- This test checks for gaps in trading data that could indicate data pipeline issues

with trading_data_checks as (
    select 
        trade_date,
        desk,
        count(*) as daily_trade_count,
        count(distinct trader_name) as unique_traders,
        sum(case when quantity * price > 1000000 then 1 else 0 end) as large_trades,
        -- Check for suspicious patterns
        case when count(*) = 0 then 'NO_TRADES_FOR_DATE_DESK' end as error_no_trades,
        case when count(distinct trader_name) = 1 and count(*) > 100 then 'SINGLE_TRADER_HIGH_VOLUME' end as error_concentration,
        case when avg(quantity * price) > 10000000 then 'EXCESSIVE_AVERAGE_TRADE_SIZE' end as error_trade_size
    from {{ ref('int_trading_data_quality') }}
    where trade_date >= current_date() - 30  -- Last 30 days
    group by trade_date, desk
),

-- Check for missing trading days (should have trades on weekdays)
expected_trading_days as (
    select 
        dateadd('day', seq4(), current_date() - 30) as expected_date
    from table(generator(rowcount => 30))
    where dayofweek(expected_date) between 2 and 6  -- Monday to Friday
),

missing_trading_days as (
    select 
        e.expected_date,
        'MISSING_TRADING_DAY' as error_type,
        'No trades found for expected trading day: ' || e.expected_date as error_message
    from expected_trading_days e
    left join (
        select distinct trade_date 
        from {{ ref('int_trading_data_quality') }}
    ) t on e.expected_date = t.trade_date
    where t.trade_date is null
),

-- Combine all completeness errors
completeness_errors as (
    select 
        trade_date as error_date,
        desk,
        error_no_trades as error_type,
        'No trades found for desk ' || desk || ' on ' || trade_date as error_message
    from trading_data_checks
    where error_no_trades is not null
    
    union all
    
    select 
        trade_date as error_date,
        desk,
        error_concentration as error_type,
        'Suspicious trading concentration for desk ' || desk || ' on ' || trade_date as error_message
    from trading_data_checks
    where error_concentration is not null
    
    union all
    
    select 
        trade_date as error_date,
        desk,
        error_trade_size as error_type,
        'Excessive average trade size for desk ' || desk || ' on ' || trade_date as error_message
    from trading_data_checks
    where error_trade_size is not null
    
    union all
    
    select 
        expected_date as error_date,
        'ALL_DESKS' as desk,
        error_type,
        error_message
    from missing_trading_days
)

-- Return records that indicate completeness issues
-- If this query returns any rows, the test will fail
select 
    error_date,
    desk,
    error_type,
    error_message,
    current_timestamp() as detected_at
from completeness_errors
order by error_date desc, desk 