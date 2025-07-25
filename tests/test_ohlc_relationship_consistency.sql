-- Custom test to ensure OHLC price relationships are valid
-- This test will fail if any records violate basic OHLC rules

with market_data_validation as (
    select 
        run_date,
        ticker,
        data_source,
        open,
        high,
        low,
        close,
        -- Check all OHLC validation rules
        case when high < low then 'HIGH_LESS_THAN_LOW' end as error_high_low,
        case when open < low or open > high then 'OPEN_OUTSIDE_RANGE' end as error_open_range,
        case when close < low or close > high then 'CLOSE_OUTSIDE_RANGE' end as error_close_range,
        case when (high - low) / ((high + low) / 2) > 0.5 then 'EXCESSIVE_VOLATILITY' end as error_volatility
    from {{ ref('int_market_data_quality') }}
),

validation_errors as (
    select
        run_date,
        ticker,
        data_source,
        open,
        high,
        low,
        close,
        array_construct(
            error_high_low,
            error_open_range,
            error_close_range,
            error_volatility
        ) as validation_errors
    from market_data_validation 
    where error_high_low is not null 
       or error_open_range is not null 
       or error_close_range is not null 
       or error_volatility is not null
)

-- Return records that have validation errors
-- If this query returns any rows, the test will fail
select 
    *,
    'OHLC validation failed for ' || ticker || ' on ' || run_date || ': ' || 
    array_to_string(validation_errors, ', ') as error_message
from validation_errors 