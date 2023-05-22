{% test expect_column_values_to_have_consistent_casing(model, column_name, display_inconsistent_columns=False) %}

with distinct_values as (

    select
        distinct {{ column_name }} as distinct_value
    from
        {{ model }}

 ),
counted_distinct_values as (
    select
        count(1) as set_count,
        count(distinct lower(distinct_value)) as set_count_case_insensitive
    from
        distinct_values

),
validation_errors as (
    select *
    from
        counted_distinct_values
    where
        set_count != set_count_case_insensitive
    
),
inconsistent_columns_validation_errors as (
    select
        lower(distinct_value) as inconsistent_columns,
        count(distinct_value) as set_count_case_insensitive
    from
        distinct_values
    group by 1
    having
        count(distinct_value) > 1
),
verbose_validation_errors as (
    select model_.*
    from {{ model }} model_
    left join  inconsistent_columns_validation_errors icve
        on lower(model_.{{ column_name }}) = icve.inconsistent_columns
    where icve.set_count_case_insensitive is not null   

)
select * 
from 
{% if display_inconsistent_columns -%}
    inconsistent_columns_validation_errors
{%- elif should_store_failures() -%}
    verbose_validation_errors
{%- else -%}
    validation_errors
{%- endif -%}

{%- endtest -%}
