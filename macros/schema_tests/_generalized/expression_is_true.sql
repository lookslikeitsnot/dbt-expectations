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
{# Check if the query is an aggregation by finding matching operator followed by balanced parentheses #}
{%- set aggregations_pattern = '(avg|count|max|min|stddev|sum)\((?:[^)(]|\((?:[^)(]|\((?:[^)(]|\([^)(]*\))*\))*\))*\)' -%}
{%- set re = modules.re -%}
{%- set inline_expression = re.sub('\s{2,}|\n', " ", expression) -%}
{%- set any_aggregation_matches = re.search(aggregations_pattern, inline_expression, re.IGNORECASE) -%}
with validation_errors as (
    select
        {% if group_by_columns %}
            {% for group_by_column in group_by_columns -%}
                {{ group_by_column }} as col_{{ loop.index }}{% if not loop.last %},{% endif %}
            {% endfor -%}
        {# if expression contains aggregations and storing failures, emit all grouped columns from model #}
        {% elif any_aggregation_matches and should_store_failures() %}
            {%- set all_aggregation_matches = re.finditer(aggregations_pattern, inline_expression, re.IGNORECASE) -%}
            {% for aggregation_matches in all_aggregation_matches -%}
                {%- set aggregation_column_name = (aggregation_matches.group(0) | replace('(', '_')| replace(')', '_')| replace('.', '_')| replace(' ', '_')| replace('*', 'star')) -%}
                {{ aggregation_matches.group(0) }}  as col_{{ loop.index }}_{{ aggregation_column_name }}{% if not loop.last %},{% endif %}
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
{% if not any_aggregation_matches %}
        and not(({{expression}}) {{ test_condition }})
{% endif %}
{%- if row_condition %}
        and {{ row_condition }}
{% endif %}
{% if group_by_columns %}
    group by {{ group_by_columns | join(", ") }}
{% endif %}
{# if the expression is an eggregation, add it as a filter in the HAVING clause #}
{% if any_aggregation_matches %}
   having not(({{expression}}) {{ test_condition }})
{% endif %}

)

select * from validation_errors
{% endmacro -%}

