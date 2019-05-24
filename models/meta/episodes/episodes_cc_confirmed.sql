with delighted_nps_patient_survey as (
        select * from {{ ref('countdown_cc_confirmed') }}
    )

    , cc_confirmed as (
        select episode_id
            , cc_code
            , is_cc_confirmed
            , row_number() over (partition by episode_id order by timestamp) as rank
         from delighted_nps_patient_survey
    )

select episode_id
    , cc_code
    , is_cc_confirmed
from cc_confirmed
where rank = 1
