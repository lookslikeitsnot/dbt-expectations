{# Get expression without special characters #}
{%- macro _replace_special_characters(expression) -%}
    {%- set clean_expression = expression | replace('(', '_')| replace(')', '_')| replace('.', '_')| replace(' ', '_')| replace('*', 'star') -%}
    
    {{ return(clean_expression) }}
{%- endmacro -%}

{# Get a list of all distinct aggregation expressions in the given expression.
    Empty if no aggregations are present in the expression. #}
{% macro _get_distinct_aggregation_list(expression) -%}
    {# Check if the query is an aggregation by finding matching operator followed by balanced parentheses #}
    {%- set aggregations_pattern = '(avg|count|max|min|stddev|sum)\((?:[^)(]|\((?:[^)(]|\((?:[^)(]|\([^)(]*\))*\))*\))*\)' -%}
    {%- set re = modules.re -%}

    {# Start by removing all extra whitespaces #}
    {%- set inline_expression = re.sub('\s{2,}|[\r\n]+', " ", expression) -%}

    {# Get all the matches #}
    {%- set all_aggregation_matches = re.finditer(aggregations_pattern, inline_expression, re.IGNORECASE) -%}

    {% set distinct_aggregation_expressions = [] %}
    {% if all_aggregation_matches %}
      {% for aggregation_matches in all_aggregation_matches if not aggregation_matches.group(0) in distinct_aggregation_expressions -%}
        {% do distinct_aggregation_expressions.append(aggregation_matches.group(0)) %}
      {% endfor -%}
    {% endif %}

    {{ return(distinct_aggregation_expressions|list) }}
{%- endmacro %}