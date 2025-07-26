-- Test to detect unusual trading volume spikes that might indicate:
-- 1. Data quality issues (duplicate records, incorrect aggregation)
-- 2. Market manipulation or unusual activity
-- 3. System errors in trade reporting

with daily_volumes as (
    select 
        trade_date,
        desk,
        sum(quantity * price) as total_daily_volume,
        count(*) as trade_count
    from {{ ref('stg_trading_books') }}
    group by trade_date, desk
),

volume_with_stats as (
    select 
        *,
        avg(total_daily_volume) over (partition by desk order by trade_date rows between 29 preceding and current row) as avg_30day_volume,
        stddev(total_daily_volume) over (partition by desk order by trade_date rows between 29 preceding and current row) as stddev_30day_volume
    from daily_volumes
),

anomalies as (
    select 
        *,
        abs(total_daily_volume - avg_30day_volume) / nullif(stddev_30day_volume, 0) as z_score,
        case 
            when total_daily_volume > avg_30day_volume * 5 then 'VOLUME_SPIKE_5X'
            when total_daily_volume < avg_30day_volume * 0.1 then 'VOLUME_DROP_90PCT'
            when abs(total_daily_volume - avg_30day_volume) / nullif(stddev_30day_volume, 0) > 3 then 'STATISTICAL_OUTLIER'
            else 'NORMAL'
        end as anomaly_type
    from volume_with_stats
    where stddev_30day_volume is not null -- Ensure we have enough data for statistics
)

select 
    trade_date,
    desk,
    total_daily_volume,
    avg_30day_volume,
    z_score,
    anomaly_type,
    trade_count
from anomalies
where anomaly_type != 'NORMAL'
  and trade_date >= current_date - interval '7 days'  -- Only flag recent anomalies 