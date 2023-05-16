{% test expect_compound_columns_to_be_unique(model,
                                                column_list,
                                                quote_columns=False,
                                                ignore_row_if="all_values_are_missing",
                                                row_condition=None
                                                ) %}
{% if not column_list %}
    {{ exceptions.raise_compiler_error(
        "`column_list` must be specified as a list of columns. Got: '" ~ column_list ~"'.'"
    ) }}
{% endif %}

{% if not quote_columns %}
    {%- set columns=column_list %}
{% elif quote_columns %}
    {%- set columns=[] %}
        {% for column in column_list -%}
            {% set columns = columns.append( adapter.quote(column) ) %}
        {%- endfor %}
{% else %}
    {{ exceptions.raise_compiler_error(
        "`quote_columns` argument for expect_compound_columns_to_be_unique test must be one of [True, False] Got: '" ~ quote_columns ~"'.'"
    ) }}
{% endif %}

{%- set row_condition_ext -%}

    {%- if row_condition %}
    {{ row_condition }} and
    {% endif -%}

    {{ dbt_expectations.ignore_row_if_expression(ignore_row_if, columns) }}

{%- endset -%}

with validation_errors as (
    select
        {% for column in columns -%}
        {{ column }} as col_{{ loop.index }} {% if not loop.last %},{% endif %}
        {%- endfor %}
    from {{ model }}
    where
        1=1
    {%- if row_condition_ext %}
        and {{ row_condition_ext }}
    {% endif %}
    group by
        {% for column in columns -%}
        {{ column }}{% if not loop.last %},{% endif %}
        {%- endfor %}
    having count(*) > 1

)
{# if storing failures, emit all columns from the model #}
{% if should_store_failures() %}
select model_.* 
from {{ model }} model_
join validation_errors ve
on 
{% for column in columns -%}
model_.{{ column }} = ve.col_{{ loop.index }} {% if not loop.last %} and {% endif %}
{%- endfor %} 
where
    ve is not null
    {%- if row_condition_ext %}
        and {{ row_condition_ext }}
    {% endif %}
{% else %}
select * from validation_errors
{% endif %}
{% endtest %}



