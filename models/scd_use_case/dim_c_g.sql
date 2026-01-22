{{ config(
    materialized='incremental',
    incremental_strategy='append',

    post_hook = [
        "{% if is_incremental() %}
        update {{ this }} tgt
        set
            end_date = src.start_date,
            is_active_flag = 'N'
        from (
            select
                customer_sk,
                to_date(load_ts) as start_date,
                record_hash
            from {{ ref('stg_c_g') }}
        ) src
        where tgt.customer_sk = src.customer_sk
          and tgt.is_active_flag = 'Y'
          and tgt.record_hash <> src.record_hash
        {% endif %}"
    ]
) }}

-- =====================================================
-- Step 1: Stage CTE
-- =====================================================
with stage as (
    select
        customer_sk,
        record_hash,
        first_name,
        last_name,
        email,
        phone,
        city,
        country,
        to_date(load_ts) as start_date
    from {{ ref('stg_c_g') }}
),

-- =====================================================
-- Step 2: Current active dim rows
-- =====================================================
current_dim as (

    {% if is_incremental() %}
        select
            customer_sk,
            record_hash
        from {{ this }}
        where is_active_flag = 'Y'
    {% else %}
        select
            null as customer_sk,
            null as record_hash
        where 1 = 0
    {% endif %}

),

-- =====================================================
-- Step 3: Identify NEW or CHANGED customers
-- =====================================================
delta as (
    select s.*
    from stage s
    left join current_dim d         --left join keeps all records from stage model which has latest data
        on s.customer_sk = d.customer_sk    --joins with existing active records based on sk column
    where d.customer_sk is null            --new records
       or s.record_hash <> d.record_hash    ---changed records
)

-- =====================================================
-- Step 4: Append new SCD rows
-- =====================================================
select
    {{ dbt_utils.generate_surrogate_key([
        'customer_sk',
        'record_hash',
        'start_date'
    ]) }} as dim_customer_id,

    customer_sk,
    record_hash,
    first_name,
    last_name,
    email,
    phone,
    city,
    country,

    start_date,
    null::date as end_date,
    'Y' as is_active_flag

from delta