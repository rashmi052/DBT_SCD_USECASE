{{ config(
    materialized = 'table'
) }}

-- =====================================================
-- Consumption model: Current active customers only
-- =====================================================

select
    first_name,
    last_name,
    email,
    phone,
    city,
    country
from {{ ref('dim_c_g') }}
where is_active_flag = 'Y'
