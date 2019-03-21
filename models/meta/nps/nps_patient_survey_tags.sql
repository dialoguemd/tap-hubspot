with
    nps as (
        select * from {{ ref('nps_patient_survey')}}
    )

    , unnested as (
        select survey_id
            , episode_id
            , user_id
            , category
            , comment
            , timestamp as created_at
            , unnest(tags) as tag
            , organization_name
            , family_member_type
            , language
            , residence_province
            , gender
        from nps
    )

select survey_id
    , episode_id
    , user_id
    , category
    , comment
    , created_at
    , case
        when tag like '+ %' then trim(leading '+ ' from tag)
        when tag like '- %' then trim(leading '- ' from tag)
        else null
        end as tag
    , case
        when tag like '+ %' then 'positive'
        when tag like '- %' then 'negative'
        else null
        end as sentiment
    , organization_name
    , family_member_type
    , language
    , residence_province
    , gender
from unnested
where tag like '+ %'
    or tag like '- %'
