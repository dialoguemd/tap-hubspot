with
  episodes as (
    select * from {{ ref('episodes') }}
  )

  , scribe_plans_detailed as (
    select * from {{ ref('scribe_plans_detailed') }}
  )

  , user_contract as (
    select * from {{ ref('user_contract') }}
  )

select episodes.first_message_patient as created_at
    , episodes.episode_id
    , episodes.user_id
    , episodes.issue_type
    , episodes.organization_name
    , episodes.messages_total
from episodes
left join scribe_plans_detailed
    using (organization_id)
left join user_contract
    on episodes.user_id = user_contract.user_id
    and episodes.first_message_patient <@ user_contract.during_est
where scribe_plans_detailed.has_mental_health
   and episodes.issue_type = 'psy'
   and episodes.first_message_patient > '2018-10-31'
   and user_contract.family_member_type = 'Employee'
