with
    nps as (
        select * from {{ ref('nps_patient_survey')}}
    )

    , tags as (
        select * from {{ ref('delighted_survey_tags_patient')}}
    )

select tags.survey_id
    , nps.episode_id
    , nps.user_id
    , nps.category
    , nps.comment
    , nps.timestamp as created_at
    , tags.tag
    , tags.sentiment
    , nps.organization_name
    , nps.contract_id
    , nps.family_member_type
    , nps.language
    , nps.residence_province
    , nps.gender
from tags
left join nps
    using (survey_id)
where tags.sentiment is not null
