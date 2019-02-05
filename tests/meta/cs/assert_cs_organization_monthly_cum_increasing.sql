with
    cs_organization_monthly as (
        select * from {{ ref('cs_organization_monthly') }}
    )

    , org_monthly as (
        select date_month
            , organization_id
            , residence_province
            , sum(total_daus_cum) as daus
            , sum(total_active_on_chat_cum) as chats
            , sum(total_active_on_video_cum) as videos
        from cs_organization_monthly
        group by 1,2,3
    )

    , org_monthly_with_lag as (
        select date_month
            , organization_id
            , residence_province
            , daus
            , coalesce(
                lag(daus) over
                    (partition by organization_id, residence_province
                        order by date_month)
                ,0) as daus_lag
            , chats
            , coalesce(
                lag(chats) over
                    (partition by organization_id, residence_province
                        order by date_month)
                ,0) as chats_lag
            , videos
            , coalesce(
                lag(videos) over
                    (partition by organization_id, residence_province
                        order by date_month)
                ,0)  as videos_lag
        from org_monthly
    )

select *
from org_monthly_with_lag
where daus < daus_lag
   or chats < chats_lag
   or videos < videos_lag
