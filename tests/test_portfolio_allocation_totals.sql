-- Test to ensure portfolio target allocations sum to 1.0 (100%) for each desk
-- This is a critical business rule - if allocations don't sum to 100%, 
-- the portfolio will be incorrectly balanced

select 
    desk,
    sum(target_allocation) as total_allocation,
    abs(sum(target_allocation) - 1.0) as allocation_variance
from {{ ref('stg_weights') }}
group by desk
having abs(sum(target_allocation) - 1.0) > 0.01  -- Allow for small rounding differences 