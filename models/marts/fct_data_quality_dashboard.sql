{{ config(
    materialized='table',
    description='Comprehensive data quality dashboard showing quarantine statistics and trends'
) }}

with trading_quarantine_summary as (
    select
        'TRADING_DATA' as data_source,
        'raw_trading_data_quarantine' as quarantine_table,
        count(*) as total_quarantined_records,
        count(distinct quarantine_run_id) as total_runs,
        max(quarantine_timestamp) as last_quarantine_run,
        min(quarantine_timestamp) as first_quarantine_run,
        failed_tests,
        -- Parse individual test failures
        array_size(failed_tests) as number_of_failed_tests,
        count(*) as records_with_this_failure_pattern
    from {{ this.database }}.{{ this.schema }}.raw_trading_data_quarantine
    where quarantine_timestamp >= current_date() - 30  -- Last 30 days
    group by failed_tests
),

market_data_quarantine_summary as (
    select
        'MARKET_DATA' as data_source,
        'combined_market_data_quarantine' as quarantine_table,
        count(*) as total_quarantined_records,
        count(distinct quarantine_run_id) as total_runs,
        max(quarantine_timestamp) as last_quarantine_run,
        min(quarantine_timestamp) as first_quarantine_run,
        failed_tests,
        array_size(failed_tests) as number_of_failed_tests,
        count(*) as records_with_this_failure_pattern
    from {{ this.database }}.{{ this.schema }}.combined_market_data_quarantine
    where quarantine_timestamp >= current_date() - 30  -- Last 30 days
    group by failed_tests
),

combined_quarantine_stats as (
    select * from trading_quarantine_summary
    union all
    select * from market_data_quarantine_summary
),

-- Calculate quality scores
data_quality_scores as (
    select
        data_source,
        quarantine_table,
        total_quarantined_records,
        total_runs,
        last_quarantine_run,
        first_quarantine_run,
        failed_tests,
        number_of_failed_tests,
        records_with_this_failure_pattern,
        -- Calculate quality score (higher is better)
        case 
            when total_quarantined_records = 0 then 100.0
            else greatest(0, 100.0 - (total_quarantined_records / 1000.0 * 100))
        end as data_quality_score,
        -- Categorize failure severity
        case
            when number_of_failed_tests >= 5 then 'CRITICAL'
            when number_of_failed_tests >= 3 then 'HIGH'
            when number_of_failed_tests >= 2 then 'MEDIUM'
            else 'LOW'
        end as failure_severity,
        -- Calculate trends
        case
            when datediff('day', first_quarantine_run, last_quarantine_run) > 0
            then total_quarantined_records / datediff('day', first_quarantine_run, last_quarantine_run)
            else 0
        end as avg_daily_quarantine_rate
    from combined_quarantine_stats
),

-- Create summary statistics
final_dashboard as (
    select
        data_source,
        quarantine_table,
        total_quarantined_records,
        total_runs,
        last_quarantine_run,
        first_quarantine_run,
        failed_tests,
        number_of_failed_tests,
        records_with_this_failure_pattern,
        data_quality_score,
        failure_severity,
        avg_daily_quarantine_rate,
        -- Add recommendations
        case
            when failure_severity = 'CRITICAL' then 'IMMEDIATE ACTION REQUIRED: Multiple data quality failures detected'
            when failure_severity = 'HIGH' then 'HIGH PRIORITY: Review data sources and validation rules'
            when failure_severity = 'MEDIUM' then 'MEDIUM PRIORITY: Monitor trends and investigate patterns'
            when failure_severity = 'LOW' then 'LOW PRIORITY: Standard monitoring sufficient'
            else 'GOOD: No major data quality issues detected'
        end as recommendation,
        -- Quality trend indicators
        case
            when avg_daily_quarantine_rate > 100 then 'DETERIORATING'
            when avg_daily_quarantine_rate > 50 then 'CONCERNING'
            when avg_daily_quarantine_rate > 10 then 'STABLE'
            else 'IMPROVING'
        end as quality_trend,
        current_timestamp() as dashboard_refresh_time
    from data_quality_scores
)

select * from final_dashboard
order by 
    case failure_severity
        when 'CRITICAL' then 1
        when 'HIGH' then 2
        when 'MEDIUM' then 3
        when 'LOW' then 4
        else 5
    end,
    data_quality_score asc,
    total_quarantined_records desc 