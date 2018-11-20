with
    active_users as (
        select * from {{ ref('active_users')}}
    )

select date_week
    , count(distinct user_id) as waus
    , count(distinct dau_id) as daus
    , count(distinct dau_id) filter(where active_on_chat) as daus_chat
    , count(distinct dau_id)
        filter(where active_on_video_gp or active_on_video_np)
        as daus_video_gp_np

    , count(distinct user_id) filter(where contract_status = 'paid') as waus_paid
    , count(distinct dau_id) filter(where contract_status = 'paid') as daus_paid
    , count(distinct dau_id)
        filter(where active_on_chat and contract_status = 'paid')
        as daus_chat_paid
    , count(distinct dau_id)
        filter(where (active_on_video_gp or active_on_video_np) and contract_status = 'paid')
    as daus_video_gp_np_paid
from active_users
group by 1
