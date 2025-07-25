-- Data Quality Framework Demonstration Script
-- This script demonstrates the comprehensive data quality capabilities of the dbt project

-- ============================================
-- STEP 1: Run Data Quality Models
-- ============================================
-- Execute the following dbt commands to run the data quality framework:
-- dbt run --models int_trading_data_quality int_market_data_quality fct_data_quality_dashboard

-- ============================================
-- STEP 2: View Clean Data Results
-- ============================================

-- Check validated trading data (only clean records)
SELECT 
    'TRADING DATA QUALITY SUMMARY' as report_section,
    COUNT(*) as clean_records,
    COUNT(DISTINCT desk) as active_desks,
    COUNT(DISTINCT trader_name) as active_traders,
    MIN(trade_date) as earliest_trade,
    MAX(trade_date) as latest_trade,
    AVG(quantity * price) as avg_trade_value
FROM int_trading_data_quality;

-- Check validated market data (only clean records)  
SELECT 
    'MARKET DATA QUALITY SUMMARY' as report_section,
    data_source,
    COUNT(*) as clean_records,
    COUNT(DISTINCT ticker) as unique_tickers,
    AVG(daily_volatility_pct) as avg_daily_volatility,
    AVG(daily_return_pct) as avg_daily_return
FROM int_market_data_quality
GROUP BY data_source;

-- ============================================
-- STEP 3: Monitor Data Quality Dashboard
-- ============================================

-- View comprehensive data quality dashboard
SELECT 
    data_source,
    quarantine_table,
    total_quarantined_records,
    data_quality_score,
    failure_severity,
    quality_trend,
    recommendation,
    last_quarantine_run
FROM fct_data_quality_dashboard
ORDER BY 
    CASE failure_severity 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'HIGH' THEN 2 
        WHEN 'MEDIUM' THEN 3 
        ELSE 4 
    END,
    data_quality_score ASC;

-- ============================================
-- STEP 4: Investigate Quarantine Tables
-- ============================================

-- Check what's in the trading data quarantine
SELECT 
    'TRADING QUARANTINE ANALYSIS' as report_section,
    failed_tests,
    COUNT(*) as record_count,
    array_to_string(failed_tests, ', ') as failure_reasons
FROM raw_trading_data_quarantine
GROUP BY failed_tests
ORDER BY COUNT(*) DESC;

-- Check what's in the market data quarantine  
SELECT 
    'MARKET DATA QUARANTINE ANALYSIS' as report_section,
    failed_tests,
    data_source,
    COUNT(*) as record_count,
    array_to_string(failed_tests, ', ') as failure_reasons
FROM combined_market_data_quarantine
GROUP BY failed_tests, data_source
ORDER BY COUNT(*) DESC;

-- ============================================
-- STEP 5: Quality Trend Analysis
-- ============================================

-- Analyze quarantine trends over time (Trading Data)
SELECT 
    'TRADING DATA QUARANTINE TRENDS' as report_section,
    DATE(quarantine_timestamp) as quarantine_date,
    COUNT(*) as daily_quarantine_count,
    COUNT(DISTINCT array_to_string(failed_tests, ',')) as unique_failure_patterns,
    array_agg(distinct array_to_string(failed_tests, ', ')) as daily_failure_types
FROM raw_trading_data_quarantine
WHERE quarantine_timestamp >= CURRENT_DATE() - 7  -- Last 7 days
GROUP BY DATE(quarantine_timestamp)
ORDER BY quarantine_date DESC;

-- Analyze quarantine trends over time (Market Data)
SELECT 
    'MARKET DATA QUARANTINE TRENDS' as report_section,
    data_source,
    DATE(quarantine_timestamp) as quarantine_date,
    COUNT(*) as daily_quarantine_count,
    COUNT(DISTINCT array_to_string(failed_tests, ',')) as unique_failure_patterns
FROM combined_market_data_quarantine
WHERE quarantine_timestamp >= CURRENT_DATE() - 7  -- Last 7 days
GROUP BY data_source, DATE(quarantine_timestamp)
ORDER BY data_source, quarantine_date DESC;

-- ============================================
-- STEP 6: Business Impact Analysis
-- ============================================

-- Calculate potential business impact of quarantined trades
SELECT 
    'QUARANTINED TRADES BUSINESS IMPACT' as report_section,
    desk,
    COUNT(*) as quarantined_trades,
    SUM(quantity * price) as potential_lost_value,
    AVG(quantity * price) as avg_quarantined_trade_value,
    array_agg(distinct array_to_string(failed_tests, ', ')) as common_failure_reasons
FROM raw_trading_data_quarantine
GROUP BY desk
ORDER BY potential_lost_value DESC;

-- ============================================
-- STEP 7: Data Quality Score Summary
-- ============================================

-- Final quality scorecard
SELECT 
    'DATA QUALITY SCORECARD' as report_section,
    ROUND(AVG(data_quality_score), 2) as overall_quality_score,
    COUNT(CASE WHEN failure_severity IN ('CRITICAL', 'HIGH') THEN 1 END) as high_priority_issues,
    COUNT(CASE WHEN quality_trend = 'DETERIORATING' THEN 1 END) as deteriorating_sources,
    COUNT(CASE WHEN data_quality_score >= 90 THEN 1 END) as excellent_quality_sources,
    COUNT(*) as total_data_sources
FROM fct_data_quality_dashboard;

-- ============================================
-- STEP 8: Recommendations for Action
-- ============================================

-- Priority actions based on quality analysis
SELECT 
    'RECOMMENDED ACTIONS' as report_section,
    data_source,
    recommendation,
    'Priority: ' || 
    CASE failure_severity 
        WHEN 'CRITICAL' THEN 'IMMEDIATE' 
        WHEN 'HIGH' THEN 'URGENT' 
        WHEN 'MEDIUM' THEN 'MODERATE'
        ELSE 'LOW'
    END as action_priority,
    total_quarantined_records as affected_records
FROM fct_data_quality_dashboard
WHERE failure_severity IN ('CRITICAL', 'HIGH', 'MEDIUM')
ORDER BY 
    CASE failure_severity 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'HIGH' THEN 2 
        WHEN 'MEDIUM' THEN 3 
        ELSE 4 
    END;

-- ============================================
-- NOTES FOR DEMO
-- ============================================
/*
This demonstration script showcases:

1. ✅ Comprehensive Data Validation: Business rules applied to trading and market data
2. ✅ Automatic Quarantine System: Failed records isolated with detailed failure tracking  
3. ✅ Quality Monitoring Dashboard: Centralized quality metrics and trends
4. ✅ Custom Test Framework: Advanced validation logic beyond standard dbt tests
5. ✅ Business Impact Analysis: Understanding the cost of data quality issues
6. ✅ Actionable Insights: Clear recommendations based on quality analysis

To run this demo:
1. Execute: dbt run --models int_trading_data_quality int_market_data_quality fct_data_quality_dashboard
2. Execute: dbt test
3. Run the queries in this script to explore the results
4. Review the quarantine tables for specific failed records
5. Monitor the dashboard for ongoing quality tracking

The framework ensures data reliability while providing complete transparency 
into quality issues and their business impact.
*/ 