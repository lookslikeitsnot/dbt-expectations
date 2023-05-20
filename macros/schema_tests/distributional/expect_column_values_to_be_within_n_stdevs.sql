{% test expect_column_values_to_be_within_n_stdevs(model,
                                  column_name,
                                  group_by=None,
                                  sigma_threshold=3
                                ) -%}
    {{
        adapter.dispatch('test_expect_column_values_to_be_within_n_stdevs', 'dbt_expectations') (
            model, column_name, group_by, sigma_threshold
        )
    }}
{%- endtest %}

{% macro default__test_expect_column_values_to_be_within_n_stdevs(model,
                                  column_name,
                                  group_by,
                                  sigma_threshold
                                ) %}

with metric_values as (

    select
        {{ group_by | join(",") ~ "," if group_by }}
        sum({{ column_name }}) as {{ column_name }}
    from
        {{ model }}
    {% if group_by -%}
    {{  dbt_expectations.group_by(group_by | length) }}
    {%- endif %}

),
metric_values_with_statistics as (

    select
        *,
        avg({{ column_name }}) over() as {{ column_name }}_average,
        stddev({{ column_name }}) over() as {{ column_name }}_stddev
    from
        metric_values

),
metric_values_z_scores as (

    select
        *,
        ({{ column_name }} - {{ column_name }}_average)/
            nullif({{ column_name }}_stddev, 0) as {{ column_name }}_sigma
    from
        metric_values_with_statistics

),
validation_errors as (
select
    *
from
    metric_values_z_scores
where
    abs({{ column_name }}_sigma) > {{ sigma_threshold }}
),
verbose_validation_errors as (
    select model_.* 
    from {{ model }} model_
    {% if group_by %}
    join validation_errors ve on
    {% for group_by_column in group_by -%}
        ve.{{ group_by_column }} = model_.{{ group_by_column }} {% if not loop.last %} and {% endif %}
    {% endfor -%}
    where ve.{{ column_name }} is not null   
    {%- else -%}
    , (select count(*) as cnt from validation_errors) ve_cnt
    where ve_cnt.cnt > 0 
    {%- endif -%}
)
select * from 
{% if should_store_failures() -%}
    verbose_validation_errors
{%- else -%}
    validation_errors
{%- endif -%}
{%- endmacro %}
