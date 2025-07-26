{{ config(
    materialized='table',
    description='Data quality dashboard showing test failures from dbt audit tables with ready-to-use queries'
) }}

{# Get the audit schema name dynamically #}
{% set audit_schema = target.schema ~ '_dbt_test__audit' %}

{# Get all audit tables dynamically #}
{% set audit_tables_query %}
  select table_name
  from information_schema.tables 
  where table_schema = '{{ audit_schema }}'
    and table_type = 'BASE TABLE'
{% endset %}

{% if execute %}
  {% set audit_tables_result = run_query(audit_tables_query) %}
  {% set audit_tables = audit_tables_result.columns[0].values() %}
{% else %}
  {% set audit_tables = [] %}
{% endif %}

with audit_table_summary as (
  {% for table in audit_tables %}
    select
      '{{ table }}' as audit_table_name,
      
      -- Parse test information from table name
      case 
        when '{{ table }}' like 'unique_%' then 'UNIQUE_CONSTRAINT'
        when '{{ table }}' like 'not_null_%' then 'NOT_NULL_CHECK' 
        when '{{ table }}' like 'accepted_values_%' then 'ACCEPTED_VALUES'
        when '{{ table }}' like 'relationships_%' then 'REFERENTIAL_INTEGRITY'
        when '{{ table }}' like 'dbt_utils_expression_is_true_%' then 'BUSINESS_RULE'
        when '{{ table }}' like 'dbt_utils_unique_combination_%' then 'COMPOSITE_UNIQUE'
        else 'OTHER_TEST'
      end as test_type,
      
      -- Extract model name from table name
      case
        when '{{ table }}' like 'unique_%' then 
          regexp_replace('{{ table }}', '^unique_([^_]+(?:_[^_]+)*)_[^_]+$', '\\1')
        when '{{ table }}' like 'not_null_%' then 
          regexp_replace('{{ table }}', '^not_null_([^_]+(?:_[^_]+)*)_[^_]+$', '\\1')
        when '{{ table }}' like 'accepted_values_%' then 
          split_part('{{ table }}', '_', 3)
        else 
          split_part('{{ table }}', '_', 2)
      end as model_name,
      
      -- Get failure statistics
      count(*) as total_failures,
      max(dbt_updated_at) as latest_failure,
      min(dbt_updated_at) as earliest_failure,
      count(distinct dbt_invocation_id) as failure_runs,
      
      -- Create ready-to-use query
      'SELECT * FROM {{ audit_schema }}.{{ table }} ORDER BY dbt_updated_at DESC LIMIT 100;' as sample_query,
      'SELECT COUNT(*) as total_bad_records FROM {{ audit_schema }}.{{ table }};' as count_query,
      'SELECT * FROM {{ audit_schema }}.{{ table }} WHERE dbt_updated_at >= CURRENT_DATE - 7;' as recent_failures_query
      
    from {{ audit_schema }}.{{ table }}
    {% if not loop.last %}union all{% endif %}
  {% endfor %}
  
  {% if audit_tables|length == 0 %}
    -- Placeholder when no audit tables exist yet
    select 
      'no_failures_detected' as audit_table_name,
      'NO_FAILURES' as test_type,
      'all_models' as model_name,
      0 as total_failures,
      null::timestamp as latest_failure,
      null::timestamp as earliest_failure,
      0 as failure_runs,
      'No data quality issues detected!' as sample_query,
      'No failures to count' as count_query,
      'No recent failures found' as recent_failures_query
    where false  -- This ensures no actual rows when there are no failures
  {% endif %}
),

model_summary as (
  select
    model_name,
    count(*) as total_test_types_failing,
    sum(total_failures) as total_model_failures,
    max(latest_failure) as model_latest_failure,
    listagg(test_type, ', ') as failing_test_types,
    
    -- Create model-level investigation queries
    'SELECT table_name, count(*) as failures FROM information_schema.tables WHERE table_schema = ''' || 
    '{{ audit_schema }}'' AND table_name LIKE ''%' || model_name || '%'' GROUP BY table_name;' as model_audit_query
    
  from audit_table_summary
  where total_failures > 0
  group by model_name
),

test_type_summary as (
  select
    test_type,
    count(*) as models_affected,
    sum(total_failures) as test_type_total_failures,
    max(latest_failure) as test_type_latest_failure,
    
    -- Priority scoring
    case test_type
      when 'UNIQUE_CONSTRAINT' then 1      -- Highest priority
      when 'NOT_NULL_CHECK' then 2
      when 'REFERENTIAL_INTEGRITY' then 3
      when 'BUSINESS_RULE' then 4
      when 'ACCEPTED_VALUES' then 5
      when 'COMPOSITE_UNIQUE' then 6
      else 7
    end as priority_score
    
  from audit_table_summary  
  where total_failures > 0
  group by test_type
),

final_dashboard as (
  select
    -- Main dashboard columns
    a.audit_table_name,
    a.test_type,
    a.model_name,
    a.total_failures,
    a.latest_failure,
    a.earliest_failure,
    a.failure_runs,
    
    -- Severity classification
    case 
      when a.total_failures > 1000 then 'CRITICAL'
      when a.total_failures > 100 then 'HIGH'
      when a.total_failures > 10 then 'MEDIUM'
      else 'LOW'
    end as severity,
    
    -- Trend analysis
    case
      when a.latest_failure >= current_date - 1 then 'RECENT' 
      when a.latest_failure >= current_date - 7 then 'THIS_WEEK'
      when a.latest_failure >= current_date - 30 then 'THIS_MONTH'
      else 'OLDER'
    end as recency,
    
    -- Ready-to-use investigation queries
    a.sample_query,
    a.count_query, 
    a.recent_failures_query,
    
    -- Model-level context
    m.total_test_types_failing,
    m.total_model_failures,
    m.failing_test_types,
    m.model_audit_query,
    
    -- Test type context  
    t.models_affected as test_type_models_affected,
    t.test_type_total_failures,
    t.priority_score,
    
    -- Recommendations
    case
      when a.total_failures > 1000 then 'URGENT: Investigate immediately - run: ' || a.sample_query
      when a.total_failures > 100 then 'HIGH PRIORITY: Review data source - run: ' || a.count_query  
      when a.total_failures > 10 then 'MEDIUM PRIORITY: Monitor trend - run: ' || a.recent_failures_query
      else 'LOW PRIORITY: Occasional failures acceptable'
    end as recommendation,
    
    current_timestamp() as dashboard_updated_at
    
  from audit_table_summary a
  left join model_summary m on a.model_name = m.model_name
  left join test_type_summary t on a.test_type = t.test_type
  where a.total_failures > 0
)

select * from final_dashboard
order by 
  priority_score,
  case severity
    when 'CRITICAL' then 1
    when 'HIGH' then 2
    when 'MEDIUM' then 3
    else 4
  end,
  total_failures desc 