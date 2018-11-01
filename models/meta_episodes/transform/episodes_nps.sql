with delighted_nps_patient_survey as (
        select * from {{ ref('delighted_nps_patient_survey') }}
    )

    , all_nps as (
        select episode_id
            , user_id
            , score
            , category
            , row_number() over (partition by episode_id order by received_at desc) as rank
         from delighted_nps_patient_survey
         where score is not null
    )

select episode_id
    , score
    , category
from all_nps
where rank = 1
