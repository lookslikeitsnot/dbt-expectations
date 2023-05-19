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
{% set all_aggregation_expressions = dbt_expectations._get_distinct_aggregation_list(expression) %}
with validation_errors as (
    select
        {% if group_by_columns %}
            {% for group_by_column in group_by_columns -%}
                {{ group_by_column }} as col_{{ loop.index }}{% if not loop.last %},{% endif %}
            {% endfor -%}
        {# if expression contains aggregations and storing failures, emit all grouped columns from model #}
        {% elif all_aggregation_expressions and should_store_failures() %}
            {% for aggregation_expression in all_aggregation_expressions -%}
                {%- set aggregation_column_name = dbt_expectations._replace_special_characters(aggregation_expression) -%}
                {{ aggregation_expression}}  as col_{{ aggregation_column_name }}{{ loop.index }}{% if not loop.last %},{% endif %}
            {% endfor -%}
        {# if storing failures, emit all columns from the model for non-grouped tests #}
        {% elif should_store_failures() %}
            model_.*
        {% else %}
        {# otherwise emit the expression result #}
        {{ dbt_expectations.truth_expression(expression) }}
        {% endif %}
    from {{ model }} model_
    where 1=1
{# if the expression is not an eggregation, add it as a filter in the WHERE clause #}
{% if not all_aggregation_expressions %}
        and not(({{expression}}) {{ test_condition }})
{% endif %}
{%- if row_condition %}
        and {{ row_condition }}
{% endif %}
{% if group_by_columns %}
    group by {{ group_by_columns | join(", ") }}
{% endif %}
{# if the expression is an eggregation, add it as a filter in the HAVING clause #}
{% if all_aggregation_expressions %}
   having not(({{expression}}) {{ test_condition }})
{% endif %}

)

select * from validation_errors
{% endmacro -%}

