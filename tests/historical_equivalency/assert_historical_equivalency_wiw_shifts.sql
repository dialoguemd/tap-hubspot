{% set facts = ['shift', 'wiw_user', 'position', 'location'] %}

with
    data as (
        select * from {{ ref('wiw_shifts') }}
        where start_day_est < current_timestamp - interval '90 days'
    )

    , historicals as (
        select * from static_historicals.wiw_shifts
    )

    , aggregated as (
        select start_day_est
         {% for fact in facts %}
         , count(distinct {{fact}}_id) as {{fact}}_count
         {% endfor %}
         , sum(hours) as hours_sum
         , current_timestamp as archived_at
        from data
        group by 1
    )

select *
from historicals
left join aggregated
    using (start_day_est)
where
    {% for fact in facts %}
    (aggregated.{{fact}}_count not between
        historicals.{{fact}}_count * 0.95
        and historicals.{{fact}}_count * 1.05)
    {% if not loop.last %} and {% endif %}
    {% endfor %}
    and (aggregated.hours_sum not between
        historicals.hours_sum * 0.95
        and historicals.hours_sum * 1.05)
