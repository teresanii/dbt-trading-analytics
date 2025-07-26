{% test ohlc_price_relationships(model, open_col, high_col, low_col, close_col) %}

    {# 
    Test to ensure OHLC price relationships are valid:
    - High should be >= Open, Low, and Close
    - Low should be <= Open, High, and Close  
    - All prices should be positive
    
    This test returns records that violate these fundamental market data rules
    #}

    select 
        *,
        case 
            when {{ high_col }} < {{ open_col }} then 'High < Open'
            when {{ high_col }} < {{ low_col }} then 'High < Low'
            when {{ high_col }} < {{ close_col }} then 'High < Close'
            when {{ low_col }} > {{ open_col }} then 'Low > Open'
            when {{ low_col }} > {{ high_col }} then 'Low > High'
            when {{ low_col }} > {{ close_col }} then 'Low > Close'
            when {{ open_col }} <= 0 then 'Open <= 0'
            when {{ high_col }} <= 0 then 'High <= 0'
            when {{ low_col }} <= 0 then 'Low <= 0'
            when {{ close_col }} <= 0 then 'Close <= 0'
            else 'Unknown violation'
        end as ohlc_violation_reason
    from {{ model }}
    where 
        {{ high_col }} < {{ open_col }}
        or {{ high_col }} < {{ low_col }}
        or {{ high_col }} < {{ close_col }}
        or {{ low_col }} > {{ open_col }}
        or {{ low_col }} > {{ high_col }}
        or {{ low_col }} > {{ close_col }}
        or {{ open_col }} <= 0
        or {{ high_col }} <= 0
        or {{ low_col }} <= 0
        or {{ close_col }} <= 0

{% endtest %} 