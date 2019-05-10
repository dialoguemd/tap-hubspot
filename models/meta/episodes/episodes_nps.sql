with delighted_nps_patient_survey as (
        select * from {{ ref('delighted_survey_patient') }}
    )

    , all_nps as (
        select episode_id
            , user_id
            , score
            , category
            , row_number() over (partition by episode_id order by timestamp desc) as rank
         from delighted_nps_patient_survey
         where score is not null
    )

select episode_id
    , score
    , category
    , score as nps_score
    , category as nps_category
from all_nps
where rank = 1
