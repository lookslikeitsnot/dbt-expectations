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
    {% set aggregations_pattern = '(avg|count|max|min|stddev|sum)\((?:[^)(]|\((?:[^)(]|\((?:[^)(]|\([^)(]*\))*\))*\))*\)' %}
    {% set re = modules.re %}
    {% set inline_expression = re.sub('\s{2,}|\n', " ", expression) %}
    {% set aggregation_matches = re.search(aggregations_pattern, inline_expression, re.IGNORECASE) %}
with grouped_expression as (
    select
        {% if group_by_columns %}
        {% for group_by_column in group_by_columns -%}
        {{ group_by_column }} as col_{{ loop.index }},
        {% endfor -%}
        {# if expression contains aggregations, display aggregated column(s) #}
        {% elif aggregation_matches and should_store_failures() %}
        {% set all_aggregation_matches = re.finditer(aggregations_pattern, inline_expression, re.IGNORECASE) %}
        {% for aggregation_matches in all_aggregation_matches -%}
        {% set aggregation_column_name = (aggregation_matches.group(0) | replace('(', '_')| replace(')', '_')| replace('.', '_')| replace(' ', '_')| replace('*', 'star')) %}
        {{ aggregation_matches.group(0) }}  as col_{{ loop.index }}_{{aggregation_column_name}},
        {% endfor -%}
        {# if storing failures, store full model if not grouping #}
        {% elif should_store_failures() %}
        model_.*,
        {% endif %}
        {{ dbt_expectations.truth_expression(expression) }}
    from {{ model }} model_
     {%- if row_condition %}
    where
        {{ row_condition }}
    {% endif %}
    {% if group_by_columns %}
    group by
    {% for group_by_column in group_by_columns -%}
        {{ group_by_column }}{% if not loop.last %},{% endif %}
    {% endfor %}
    {% endif %}

),
validation_errors as (

    select
        *
    from
        grouped_expression
    where
        not(expression {{ test_condition }})

)

select *
from validation_errors


{% endmacro -%}
