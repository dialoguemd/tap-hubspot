with
    cp_activity as (
        select * from {{ ref('cp_activity') }}
    )

    , episodes_outcome as (
        select * from {{ ref('episodes_outcomes') }}
    )

    , episodes_issue_type as (
        select * from {{ ref('episodes_issue_types') }}
    )

    , user_contract as (
        select * from {{ ref('user_contract') }}
    )

select cp_activity.date
    , cp_activity.activity_start
    , cp_activity.activity_end
    , cp_activity.activity
    , cp_activity.time_spent as time_spent_seconds
    , cp_activity.patient_id
    , cp_activity.user_id as care_team_member_id
    , cp_activity.episode_id
    , cp_activity.main_specialization
    , cp_activity.shift_position
    , cp_activity.shift_location
    , cp_activity.is_active
    , episodes_outcome.outcome as episode_outcome
    , episodes_issue_type.issue_type as episode_issue_type
    , user_contract.organization_name
    , user_contract.account_name
    , user_contract.account_industry
from cp_activity
left join episodes_outcome
    using (episode_id)
left join episodes_issue_type
    using (episode_id)
left join user_contract
    on cp_activity.patient_id = user_contract.user_id
        and cp_activity.activity_start <@ user_contract.during_est
