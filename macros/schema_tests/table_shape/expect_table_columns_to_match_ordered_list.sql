{%- test expect_table_columns_to_match_ordered_list(model, column_list, transform="upper") -%}
{% if not execute %}
    {{ return('') }}
{% endif %}
{%- set column_list = column_list | map(transform) | list -%}
{%- set relation_column_names = dbt_expectations._get_column_list(model, transform) -%}
{%- set matching_columns = dbt_expectations._list_intersect(column_list, relation_column_names) -%}
with relation_columns as (

    {% for col_name in relation_column_names %}
    select
        {{ loop.index }} as relation_column_idx,
        cast('{{ col_name }}' as {{ dbt.type_string() }}) as relation_column
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),
input_columns as (

    {% for col_name in column_list %}
    select
        {{ loop.index }} as input_column_idx,
        cast('{{ col_name }}' as {{ dbt.type_string() }}) as input_column
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)
,
validation_errors as (

    select *
    from
        relation_columns r
        full outer join
        input_columns i on r.relation_column = i.input_column and r.relation_column_idx = i.input_column_idx
    where
        -- catch any column in input list that is not in the sequence of table columns
        -- or any table column that is not in the input sequence
        r.relation_column is null or
        i.input_column is null
),
verbose_validation_errors as (

    select
        coalesce(relation_column_idx, input_column_idx),
        relation_column,
        input_column,
        case 
            when 
                relation_column = input_column 
                then true
            else
                false
        end as matching_columns
    from 
        relation_columns rc
        full outer join
        input_columns ic on rc.relation_column_idx = ic.input_column_idx
    {# Don't store anything if there are no errors #}
    where (select count(*) from validation_errors) > 0
    order by 1 asc
)
select * 
from 
{% if should_store_failures() -%}
    verbose_validation_errors
{%- else -%}
    validation_errors
{%- endif -%}

{%- endtest -%}
