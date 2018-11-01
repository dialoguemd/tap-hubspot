with careplatform_episode_properties_updated as (
        select * from {{ ref('careplatform_episode_properties_updated') }}
    )

    , outcome_rank_valid as (
        select episode_id
            , episode_property_value as outcome
            , updated_at
            , first_value(episode_property_value) over(partition by episode_id order by updated_at desc
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first
            , last_value(episode_property_value) over(partition by episode_id order by updated_at desc
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as current
            , row_number() over(partition by episode_id order by updated_at desc) as rank
        from careplatform_episode_properties_updated
        where episode_property_type = 'outcome'
            and episode_property_value not in
                ('closed_after_follow_up',
                 'other',
                 'patient_thanks',
                 'patient_unresponsive',
                 'follow_up',
                 'audit'
            )
    )

    , outcome_rank_all as (
        select episode_id
            , episode_property_value as outcome
            , updated_at
            , first_value(episode_property_value) over(partition by episode_id order by updated_at
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as first
            , last_value(episode_property_value) over(partition by episode_id order by updated_at
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as current
            , row_number() over(partition by episode_id order by updated_at desc) as rank
        from careplatform_episode_properties_updated
        where episode_property_type = 'outcome'
    )

    , episode_outcome as (
        select outcome_rank_all.episode_id
            , min(outcome_rank_all.updated_at) as updated_at
            , outcome_rank_all.first as first_outcome
            , case
                when outcome_rank_all.first in
                    ('care_plan',
                     'prescription_pharmacy',
                     'prescription_delivery',
                     'referral_specialist',
                     'advice',
                     'ubisoft_appointment',
                     'virtual_appointment'
                    ) then 'Diagnostic'
                when outcome_rank_all.first in
                    ('referral_walk_in',
                     'referral_er',
                     'navigation'
                    ) then 'Navigation'
                when outcome_rank_all.first in
                    ('patient_unresponsive',
                     'inappropriate_profile',
                     'patient_thanks',
                     'closed_after_follow_up',
                     'follow_up',
                     'international_consult',
                     'episode_duplicate',
                     'new_dependant'
                    ) then 'Unsuitable episode'
                when outcome_rank_all.first in
                    ('referral_without_navigation',
                     'admin',
                     'test',
                     'audit',
                     'other'
                    ) then 'Other'
                else null
            end as first_outcome_category
            , coalesce(outcome_rank_valid.current, outcome_rank_all.current) as outcome
            , case
                when coalesce(outcome_rank_valid.current, outcome_rank_all.current) in
                    ('care_plan',
                     'prescription_pharmacy',
                     'prescription_delivery',
                     'referral_specialist',
                     'advice',
                     'ubisoft_appointment',
                     'virtual_appointment'
                    ) then 'Diagnostic'
                when coalesce(outcome_rank_valid.current, outcome_rank_all.current) in
                    ('referral_walk_in',
                     'referral_er',
                     'navigation'
                    ) then 'Navigation'
                when coalesce(outcome_rank_valid.current, outcome_rank_all.current) in
                    ('patient_unresponsive',
                     'inappropriate_profile',
                     'patient_thanks',
                     'closed_after_follow_up',
                     'follow_up',
                     'international_consult',
                     'episode_duplicate',
                     'new_dependant'
                    ) then 'Unsuitable episode'
                when coalesce(outcome_rank_valid.current, outcome_rank_all.current) in
                    ('referral_without_navigation',
                     'admin',
                     'test',
                     'audit',
                     'other'
                    ) then 'Other'
                else null
            end as outcome_category
            , array_agg(outcome_rank_all.outcome order by outcome_rank_all.updated_at asc) as outcomes_ordered
            , count(outcome_rank_all.outcome)
        from outcome_rank_all
        left join outcome_rank_valid
            on outcome_rank_all.episode_id = outcome_rank_valid.episode_id
            and outcome_rank_valid.rank = 1
        group by 1,3,4,5,6
    )

select episode_id
    , first_outcome_category
    , first_outcome
    , outcome_category
    , outcome
    , outcomes_ordered
    , updated_at as outcome_first_set_timestamp
from episode_outcome
