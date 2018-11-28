with patientapp_answer_submitted as (
        select * from {{ ref('patientapp_answer_submitted') }}
    )

    , all_ratings as (
        select episode_id
            , user_id
            , rating
            , row_number() over (
                partition by episode_id order by timestamp desc
            ) as rank
         from patientapp_answer_submitted
         where rating is not null
    )

select episode_id
    , rating
from all_ratings
where rank = 1
