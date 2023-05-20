{%- test expect_table_columns_to_contain_set(model, column_list, transform="upper") -%}
{% if not execute %}
    {{ return('') }}
{% endif %}
{%- set column_list = column_list | map(transform) | list -%}
{%- set relation_column_names = dbt_expectations._get_column_list(model, transform) -%}
with relation_columns as (

    {% for col_name in relation_column_names %}
    select cast('{{ col_name }}' as {{ dbt.type_string() }}) as relation_column
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),
input_columns as (

    {% for col_name in column_list %}
    select cast('{{ col_name }}' as {{ dbt.type_string() }}) as input_column
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),
validation_errors as (

    select *
    from
        input_columns i
        left join
        relation_columns r on r.relation_column = i.input_column
    where
        -- catch any column in input list that is not in the list of table columns
        r.relation_column is null
),
verbose_validation_errors as (

    select 
        relation_column,
        input_column,
        case 
            when 
                relation_column is not null and input_column is not null
                then true
            else
                false
        end as column_contained
    from 
        relation_columns rc
        full outer join
        input_columns ic on rc.relation_column = ic.input_column
    {# Don't store anything if there are no errors #}
    where (select count(*) from validation_errors) > 0
)
select * 
from 
{% if should_store_failures() -%}
    verbose_validation_errors
{%- else -%}
    validation_errors
{%- endif -%}

{%- endtest -%}
