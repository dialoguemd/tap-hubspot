select channel_id as episode_id
    , user_id
    , answers
    , case
        when answers like '[0]' then 0
        when answers like '[1]' then 1
        when answers like '[2]' then 2
        when answers like '[3]' then 3
        when answers like '[4]' then 4
        when answers like '[5]' then 5
        end as rating
    , timestamp as submitted_at
from patientapp.submit_answer_post_request
