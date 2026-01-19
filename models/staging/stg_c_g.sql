{{ config(materialized='view') }}

-- =====================================================
-- Step 1: Combine all sources
-- =====================================================
with all_sources as (

    select 
        first_underscore_name as first_name,
        last_name as last_name,
        email as email,
        phone_number as phone,
        city as city,
        country as country,
        convert_timezone('UTC', updated_at)::timestamp_ntz as load_ts
    from {{ source('sources_c_g', 'c_source_1') }}

    union all

    select 
        first_name_underscore as first_name,
        last as last_name,
        email_address as email,
        phone as phone,
        city_name as city,
        country_name as country,
        convert_timezone('UTC', updated_at)::timestamp_ntz as load_ts
    from {{ source('sources_c_g', 'c_source_2') }}

    union all

    select 
        name as first_name,
        last_name as last_name,
        email as email,
        phone as phone,
        city as city,
        cast(null as varchar) as country,
        convert_timezone('UTC', updated_at)::timestamp_ntz as load_ts
    from {{ source('sources_c_g', 'c_source_3') }}


    union all

    select 
        first_name as first_name,
        last_name as last_name,
        email as email,
        phone as phone,
        city as city,
        country as country,
        convert_timezone('UTC', updated_at)::timestamp_ntz as load_ts
    from {{ source('source_largerdataset', 'records_1000') }}



    union all

    select 
        first_name as first_name,
        last_name as last_name,
        email as email,
        phone as phone,
        city as city,
        country as country,
        convert_timezone('UTC', updated_at)::timestamp_ntz as load_ts
    from {{ source('source_largerdataset', 'records_10k') }}
),

-- =====================================================
-- Step 2: Generate keys
-- =====================================================
keyed as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'email'
        ]) }} as customer_sk,

        {{ dbt_utils.generate_surrogate_key([
            'first_name',
            'last_name',
            'phone',
            'city',
            'country'
        ]) }} as record_hash,

        first_name,
        last_name,
        email,
        phone,
        city,
        country,
        load_ts
    from all_sources
),

-- =====================================================
-- Step 3: Latest record per customer
-- =====================================================
deduped as (

    select *
    from (
        select *,
            row_number() over (
                partition by customer_sk
                order by load_ts desc
            ) as rn
        from keyed
    )
    where rn = 1
)

select
    customer_sk,
    record_hash,
    first_name,
    last_name,
    email,
    phone,
    city,
    country,
    load_ts
from deduped


