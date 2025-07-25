# Data Quality Framework

This dbt project includes a comprehensive data quality framework that validates trading and market data, automatically quarantines failed records, and provides monitoring capabilities.

## 🎯 Framework Overview

### Components
1. **Quarantine Macro** (`macros/quarantine_handler.sql`)
2. **Trading Data Quality Model** (`models/intermediate/int_trading_data_quality.sql`)
3. **Market Data Quality Model** (`models/intermediate/int_market_data_quality.sql`)
4. **Data Quality Dashboard** (`models/marts/fct_data_quality_dashboard.sql`)
5. **Custom Tests** (`tests/`)
6. **Schema Tests** (`models/schema.yml`)

## 🔧 How It Works

### 1. Data Validation Process
When data quality models run, they:
- Apply comprehensive business rule validations
- Automatically quarantine records that fail any test
- Return only clean, validated records
- Log quarantine statistics and metadata

### 2. Quarantine System
Failed records are stored in quarantine tables with:
- **Original data**: All original columns preserved
- **Quarantine metadata**: Timestamp, run ID, failed test details
- **Test failure tracking**: Array of specific tests that failed

### 3. Monitoring Dashboard
The `fct_data_quality_dashboard` provides:
- Quality scores (0-100 scale)
- Failure severity classification
- Trend analysis
- Actionable recommendations

## 📊 Data Quality Tests

### Trading Data Validations
- **Trade ID**: Not null, non-empty
- **Trade Date**: Valid date range (2020-present, not future)
- **Trader Name**: Not null, non-empty
- **Desk**: Valid desk names only
- **Ticker**: Not null, format validation
- **Quantity**: Positive, reasonable limits (≤10M)
- **Price**: Positive, reasonable limits (≤$50K)
- **Trade Type**: BUY or SELL only
- **Trade Value**: Maximum $100M per trade
- **Ticker Format**: FX pairs must be XXX/XXX format

### Market Data Validations (OHLC)
- **Date**: Valid date range
- **Ticker**: Not null, non-empty
- **OHLC Values**: All positive, not null
- **OHLC Relationships**: High ≥ Low, Open/Close within High-Low range
- **Price Ranges**: Reasonable limits (stocks ≤$10K, forex ≤10)
- **Volatility**: Daily volatility ≤50%
- **Volume**: Non-negative (stocks only)
- **Forex Format**: Currency pairs in XXX/XXX format

## 🚀 Usage Instructions

### Running Data Quality Checks
```bash
# Run all data quality models
dbt run --models +int_trading_data_quality +int_market_data_quality

# Run the monitoring dashboard
dbt run --models fct_data_quality_dashboard

# Run all tests (including custom tests)
dbt test

# Run only data quality tests
dbt test --models int_trading_data_quality int_market_data_quality
```

### Monitoring Quarantine Tables
```sql
-- Check trading data quarantine
SELECT * FROM raw_trading_data_quarantine 
ORDER BY quarantine_timestamp DESC;

-- Check market data quarantine  
SELECT * FROM combined_market_data_quarantine 
ORDER BY quarantine_timestamp DESC;

-- View data quality dashboard
SELECT * FROM fct_data_quality_dashboard 
ORDER BY failure_severity, data_quality_score;
```

### Using the Quarantine Macro
```sql
-- Example usage in a new model
{{ config(materialized='table') }}

with source_data as (
    select * from {{ source('your_source', 'your_table') }}
),

validated_data as (
    {{
        quarantine_failed_records(
            'source_data',
            [
                {
                    'name': 'valid_id',
                    'test': 'id is not null'
                },
                {
                    'name': 'positive_amount',
                    'test': 'amount > 0'
                }
            ],
            'id'
        )
    }}
)

select * from validated_data
```

## 📋 Quality Metrics

### Quality Score Calculation
- **100**: Perfect quality (no quarantined records)
- **90-99**: Excellent quality (minimal issues)
- **70-89**: Good quality (minor issues)
- **50-69**: Fair quality (moderate issues)
- **0-49**: Poor quality (significant issues)

### Severity Levels
- **CRITICAL**: 5+ failed tests per record
- **HIGH**: 3-4 failed tests per record
- **MEDIUM**: 2 failed tests per record
- **LOW**: 1 failed test per record

### Trend Indicators
- **IMPROVING**: <10 records/day quarantined
- **STABLE**: 10-50 records/day quarantined
- **CONCERNING**: 50-100 records/day quarantined
- **DETERIORATING**: 100+ records/day quarantined

## 🔍 Custom Tests

### OHLC Relationship Consistency
Tests that all OHLC price relationships are mathematically valid across market data.

### Trading Data Completeness
Validates that:
- No missing trading days (weekdays)
- No suspicious trading patterns
- Reasonable trade size distributions

## 🛠️ Customization

### Adding New Validations
1. Update the relevant quality model with new test conditions
2. Add corresponding schema tests in `models/schema.yml`
3. Update documentation

### Creating New Quality Models
1. Use the `quarantine_failed_records` macro
2. Define appropriate test conditions
3. Add to the dashboard monitoring
4. Create corresponding schema tests

## 📈 Best Practices

### For Data Engineers
- Run quality checks before downstream transformations
- Monitor quarantine tables daily
- Set up alerts for CRITICAL and HIGH severity issues
- Review and adjust validation rules quarterly

### For Analysts
- Use only validated data models (those with `_quality` suffix)
- Check the dashboard for data reliability before analysis
- Report unusual patterns in quarantine data

### For Data Stewards
- Review failed records weekly
- Investigate root causes of quality issues
- Update validation rules based on business requirements
- Maintain documentation of quality standards

## 🚨 Alerting & Monitoring

### Recommended Monitoring
- Daily quarantine record counts
- Quality score trends
- Failure pattern analysis
- Critical/High severity alerts

### Integration Points
The framework integrates with:
- dbt Cloud (native test reporting)
- BI tools (dashboard model)
- Data catalogs (metadata exposure)
- Monitoring systems (log parsing)

---

*This framework ensures data reliability and provides transparency into data quality issues, enabling proactive data management and higher confidence in analytical results.* 