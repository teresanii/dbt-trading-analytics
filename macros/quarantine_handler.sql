{% macro quarantine_failed_records(table_name, test_conditions, primary_key='id') %}
    {% set quarantine_table = table_name ~ '_quarantine' %}
    
    -- Create or replace the quarantine table
    {{ log("Creating quarantine table: " ~ quarantine_table, info=True) }}
    
    create or replace table {{ quarantine_table }} as (
        select 
            *,
            current_timestamp() as quarantine_timestamp,
            '{{ invocation_id }}' as quarantine_run_id,
            array_construct(
                {% for condition in test_conditions %}
                    case when not ({{ condition.test }}) then '{{ condition.name }}' else null end
                    {%- if not loop.last -%},{%- endif -%}
                {% endfor %}
            ) as failed_tests
        from {{ table_name }}
        where not (
            {% for condition in test_conditions %}
                ({{ condition.test }})
                {%- if not loop.last %} and {% endif -%}
            {% endfor %}
        )
    );

    -- Return clean records only
    select 
        *
    from {{ table_name }}
    where (
        {% for condition in test_conditions %}
            ({{ condition.test }})
            {%- if not loop.last %} and {% endif -%}
        {% endfor %}
    )

{% endmacro %}

{% macro get_quarantine_summary(table_name) %}
    {% set quarantine_table = table_name ~ '_quarantine' %}
    
    select 
        '{{ table_name }}' as source_table,
        count(*) as quarantine_record_count,
        max(quarantine_timestamp) as last_quarantine_run,
        quarantine_run_id,
        failed_tests,
        count(*) as records_per_failure
    from {{ quarantine_table }}
    group by quarantine_run_id, failed_tests
    order by quarantine_timestamp desc

{% endmacro %} 