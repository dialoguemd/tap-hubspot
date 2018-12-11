with
    activity as (
        select * from {{ ref('cs_activity_by_org_monthly')}}
    )

select organization_id
    , date_month

    -- Jinja loop for keeping window functions clean
    {% for field in 
        ["total_daus",
        "total_active_on_chat",
        "total_active_on_video",
        "total_active_on_video_gp",
        "total_daus_employee",
        "total_active_on_chat_employee",
        "total_active_on_video_employee",
        "total_active_on_video_gp_employee",
        "total_daus_dependent",
        "total_active_on_chat_dependent",
        "total_active_on_video_dependent",
        "total_active_on_video_gp_dependent",
        "total_daus_child",
        "total_active_on_chat_child",
        "total_active_on_video_child",
        "total_active_on_video_gp_child"] 
    %}

    , sum({{field}}) over (partition by organization_id order by date_month)
        as {{field}}_cum

    {% endfor %}

from activity
