{% macro historical_equivalency_count_test(facts, dimensions, model_name, sensitivity) %}

with
    data as (
        select * from analytics.{{model_name}}
    )

    , historicals as (
        select * from static_historicals.{{model_name}}
    )

    , aggregated as (
        select {% for dimension in dimensions %}
            {{dimension}} {% if not loop.last %} , {% endif %}
            {% endfor %}
            {% for fact in facts %}
            , count(distinct {{fact}}_id) as {{fact}}_count
            {% endfor %}
            , current_timestamp as archived_at
        from data
        {{ dbt_utils.group_by(dimensions|length) }}
    )

select *
from historicals
left join aggregated
    using (
        {% for dimension in dimensions %}
        {{dimension}} {% if not loop.last %} , {% endif %}
        {% endfor %}
    )
where
    {% for fact in facts %}
    (aggregated.{{fact}}_count not between
        historicals.{{fact}}_count * (1 - {{sensitivity}})
        and historicals.{{fact}}_count * (1 + {{sensitivity}}))
    {% if not loop.last %} and {% endif %}
    {% endfor %}

{% endmacro %}
