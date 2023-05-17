{% test expect_select_column_values_to_be_unique_within_record(model,
                                                    column_list,
                                                    quote_columns=False,
                                                    ignore_row_if="all_values_are_missing",
                                                    row_condition=None
                                                    )  -%}
    {{ adapter.dispatch('test_expect_select_column_values_to_be_unique_within_record', 'dbt_expectations') (model, column_list, quote_columns, ignore_row_if, row_condition) }}
{%- endtest %}

{% macro default__test_expect_select_column_values_to_be_unique_within_record(model,
                                                    column_list,
                                                    quote_columns,
                                                    ignore_row_if,
                                                    row_condition
                                                    ) %}

{% if not quote_columns %}
    {%- set columns=column_list %}
{% elif quote_columns %}
    {%- set columns=[] %}
        {% for column in column_list -%}
            {% set columns = columns.append( adapter.quote(column) ) %}
        {%- endfor %}
{% else %}
    {{ exceptions.raise_compiler_error(
        "`quote_columns` argument for unique_combination_of_columns test must be one of [True, False] Got: '" ~ quote_columns ~"'.'"
    ) }}
{% endif %}

{%- set row_condition_ext -%}

    {%- if row_condition  %}
    {{ row_condition }} and
    {% endif -%}

    {{ dbt_expectations.ignore_row_if_expression(ignore_row_if, columns) }}

{%- endset -%}

with indexed_filtered_model as (
    select
        row_number() over(order by 1) as row_index,
        model_.*
    from {{ model }} model_
    where
        1=1 
    {%- if row_condition_ext %}
        and {{ row_condition_ext }}
    {%- endif -%}

),
column_values as (
    select
        row_index,
        {% for column in columns -%}
        {{ column }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    from indexed_filtered_model
),
unpivot_columns as (
    {% for column in columns %}
    select row_index, '{{ column }}' as column_name, {{ column }} as column_value from column_values
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),
non_unique_values as (
    select
        row_index,
        count(distinct column_value) as column_values
    from unpivot_columns
    group by 1
    having count(distinct column_value) < {{ columns | length }}
),
validation_errors as (
{%- if should_store_failures() -%}
{%- set model_column_names = dbt_expectations._get_column_list(model, "upper") -%}
    select
    {%- for model_column_name in model_column_names %}
    tv.{{model_column_name}}{% if not loop.last %}, {% endif %}
    {%- endfor %}
    from  indexed_filtered_model tv
    join non_unique_values nuv
    on
        tv.row_index = nuv.row_index
    where nuv is not null
{%- else -%}
    select
        *
    from non_unique_values
{%- endif -%}
)
select * from validation_errors
{% endmacro %}
