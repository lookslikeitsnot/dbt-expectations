{% test expression_is_true(model,
                                 expression,
                                 test_condition="= true",
                                 group_by_columns=None,
                                 row_condition=None
                                 ) %}

{{ dbt_expectations.expression_is_true(model, expression, test_condition, group_by_columns, row_condition) }}

{% endtest %}



{% macro expression_is_true(model,
                                 expression,
                                 test_condition="= true",
                                 group_by_columns=None,
                                 row_condition=None
                                 ) %}
{{ adapter.dispatch('expression_is_true', 'dbt_expectations') (model, expression, test_condition, group_by_columns, row_condition) }}
{%- endmacro %}

{% macro default__expression_is_true(model, expression, test_condition, group_by_columns, row_condition) -%}
{%- set all_aggregation_expressions = dbt_expectations._get_distinct_aggregation_list(expression) -%}
{%- set model_column_names = dbt_expectations._get_column_list(model, "upper") -%}
with grouped_expression as (
    select
        {% if group_by_columns %}
            {% for group_by_column in group_by_columns -%}
                {{ group_by_column }} as col_{{ loop.index }},
            {% endfor -%}
        {% endif %}
        {{ dbt_expectations.truth_expression(expression) }}
    from {{ model }} model_
    where 1=1
    {%- if row_condition %}
        and {{ row_condition }}
    {% endif %}
    {% if group_by_columns %}
        group by {{ group_by_columns | join(", ") }}
    {% endif %}
),
verbose_grouped_expression as (
    select
        {% if group_by_columns %}
            {% for group_by_column in group_by_columns -%}
                {{ group_by_column }} as col_{{ loop.index }},
            {% endfor -%}
        {# if expression contains aggregations and storing failures, emit all grouped columns from model #}
        {% elif all_aggregation_expressions %}
            {% for aggregation_expression in all_aggregation_expressions -%}
                {%- set aggregation_column_name = dbt_expectations._replace_special_characters(aggregation_expression) -%}
                {{ aggregation_expression}}  as col_{{ aggregation_column_name }}{{ loop.index }},
            {% endfor -%}
            {# We need to evaluate the expression to be able to filter, using HAVING doesn't work for window functions 
                and Postgres doesn't supper QUALIFY #}
        {# if storing failures, emit all columns from the model for non-grouped tests #}
        {% else %}
            model_.*,
        {% endif %}
        {{ dbt_expectations.truth_expression(expression) }}
    from {{ model }} model_
    where 1=1
    {%- if row_condition %}
        and {{ row_condition }}
    {% endif %}
    {% if group_by_columns %}
        group by {{ group_by_columns | join(", ") }}
    {% endif %}
),
validation_errors as (
    select *
    from grouped_expression
    where not(expression {{ test_condition }})
),
{# To prevent the extra 'expression' column, reselect only wanted columns #}
verbose_validation_errors as (
    select
        {% if group_by_columns %}
            {% for group_by_column in group_by_columns -%}
                col_{{ loop.index }}{% if not loop.last %},{% endif %}
            {% endfor -%}
        {# if expression contains aggregations and storing failures, emit all grouped columns from model #}
        {% elif all_aggregation_expressions %}
            {% for aggregation_expression in all_aggregation_expressions -%}
                {%- set aggregation_column_name = dbt_expectations._replace_special_characters(aggregation_expression) -%}
                col_{{ aggregation_column_name }}{{ loop.index }}{% if not loop.last %},{% endif %}
            {% endfor -%}
        {# if storing failures, emit all columns from the model for non-grouped tests #}
        {% else %}
            {{ model_column_names | join(", ") }}
        {% endif %}
    from verbose_grouped_expression
    where not(expression {{ test_condition }})
)
select * 
from 
{% if should_store_failures() -%}
    verbose_validation_errors
{%- else -%}
    validation_errors
{%- endif -%}

{% endmacro -%}

