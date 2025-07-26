{% test portfolio_allocation_sum(model, allocation_column, group_by_column, expected_sum=1.0, tolerance=0.01) %}

    {# 
    Test to ensure portfolio allocations sum to expected value within tolerance
    
    Args:
        model: The model to test
        allocation_column: Column containing allocation values
        group_by_column: Column to group by (e.g., desk, region)
        expected_sum: Expected sum value (default 1.0 for 100%)
        tolerance: Acceptable variance from expected sum (default 0.01)
    
    This test returns groups where allocations don't sum to the expected value
    #}

    select 
        {{ group_by_column }},
        sum({{ allocation_column }}) as actual_sum,
        {{ expected_sum }} as expected_sum,
        abs(sum({{ allocation_column }}) - {{ expected_sum }}) as variance,
        case 
            when sum({{ allocation_column }}) > {{ expected_sum }} + {{ tolerance }} then 'OVER_ALLOCATED'
            when sum({{ allocation_column }}) < {{ expected_sum }} - {{ tolerance }} then 'UNDER_ALLOCATED'
            else 'WITHIN_TOLERANCE'
        end as allocation_status
    from {{ model }}
    group by {{ group_by_column }}
    having abs(sum({{ allocation_column }}) - {{ expected_sum }}) > {{ tolerance }}

{% endtest %} 