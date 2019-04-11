-- Default active hourly rate
-- rates are higher than paid hourly rate to account for active time percentage
{% set roles = [['cc', 40], ['nc', 75], ['np', 100]] %}
with
    time_spent as (
        select * from {{ ref('costs_time_spent_by_episode_daily') }}
    )

    , hourly_cost as (
        select * from {{ ref('costs_hourly_by_spec_monthly') }}
    )

    , chats as (
        select * from {{ ref('chats') }}
    )

    , episodes_subject as (
        select * from {{ ref('episodes_subject') }}
    )

select time_spent.episode_id
    , time_spent.date
    , episodes_subject.episode_subject as patient_id
    , chats.chat_type
{% for role, default_hourly_rate in roles %}
    -- replace with fixed rate for current month
    , time_spent.{{role}}_time
        * coalesce(
            hourly_cost.{{role}}_hourly,
            {{default_hourly_rate}}
        ) as {{role}}_cost
    , time_spent.{{role}}_time
        * coalesce(
            hourly_cost.{{role}}_hourly_ops,
            -- Adding 25% ops premium costs on top of the base hourly rate
            {{default_hourly_rate}} * 1.25
        ) as {{role}}_cost_ops
    , time_spent.{{role}}_time

{% endfor %}
from time_spent
left join episodes_subject
    using (episode_id)
left join chats
    on time_spent.episode_id = chats.episode_id
    and time_spent.date = chats.date_day_est
left join hourly_cost
	on date_trunc('month', time_spent.date)
        = hourly_cost.date_month
