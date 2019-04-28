
{{
  config(
    materialized='incremental',
    unique_key='shift_id',
    post_hook=[
       "{{ postgres.index(this, 'shift_id')}}",
    ]
  )
}}

with
    assignments_tmp as (
        select * from {{ ref('assigned_time_detailed') }}
        {% if is_incremental() %}
        where assigned_at_est > (select max(date_day) from {{ this }})
        {% endif %}
    )

    , shifts as (
        select * from {{ ref('wiw_shifts') }}
    )

    , episodes as (
        select * from {{ ref('episodes') }}
    )

    , videos as (
        select * from {{ ref('videos_detailed') }}
    )

    , phone_calls as (
        select * from {{ ref('telephone_calls') }}
    )

    , assignments as (
        select assignments_tmp.assignment_id
            , assignments_tmp.assigned_at_est
            , assignments_tmp.assigned_user_id
            , sum(assignments_tmp.first_response_time_min) as frt_sum
            , count(assignments_tmp.first_response_time_min) as frt_count
            , sum(assignments_tmp.assigned_time_min) as assigned_time_sum
            , count(assignments_tmp.assigned_time_min) as assigned_time_count
            , sum(assignments_tmp.rt_sum) as rt_sum
            , sum(assignments_tmp.rt_count) as rt_count
            , count(videos.*) as videos_count
            , sum(videos.video_length) as video_length_sum
            , count(phone_calls.*) as phone_calls_count
            , sum(phone_calls.call_duration) as phone_calls_length_sum

            , count(distinct episodes.episode_id)
                filter (where date_trunc('day', episodes.first_message_patient)
                    = date_trunc('day', assignments_tmp.assigned_at_est))
                as new_episode_count
            , count(episodes.score)
                filter (where episodes.category = 'promoter')
                as count_promoters
            , count(episodes.score)
                filter (where episodes.category = 'detractor')
                as count_detractors
            , count(episodes.score)
                filter (where episodes.category is not null)
                as count_scores

            , sum(assignments_tmp.assigned_time_min)
                filter (where
                    assignments_tmp.count_posts > 3
                    or videos.* is not null
                    or phone_calls.* is not null)
                as filtered_assigned_time_sum
            , count(assignments_tmp.assigned_time_min)
                filter (where
                    assignments_tmp.count_posts > 3
                    or videos.* is not null
                    or phone_calls.* is not null)
                as filtered_assigned_time_count
        from assignments_tmp
        left join episodes
            using (episode_id)
        left join videos
            on assignments_tmp.assigned_user_id = videos.careplatform_user_id
            and assignments_tmp.episode_id = videos.episode_id
            and assignments_tmp.assigned_range_est @> videos.started_at_est
        left join phone_calls
            on assignments_tmp.assigned_user_id = phone_calls.user_id
            and assignments_tmp.episode_id = phone_calls.episode_id
            and assignments_tmp.assigned_range_est @> phone_calls.started_at_est
        group by 1,2,3
    )

select shifts.start_date_est as date_day
    , shifts.shift_id
    -- Aggregate assignments to shifts
    {% for field in [
        'frt_sum',
        'frt_count',
        'assigned_time_sum',
        'assigned_time_count',
        'rt_sum',
        'rt_count',
        'videos_count',
        'video_length_sum',
        'phone_calls_count',
        'phone_calls_length_sum',
        'new_episode_count',
        'count_promoters',
        'count_detractors',
        'count_scores',
        'filtered_assigned_time_sum',
        'filtered_assigned_time_count'
        ]
    %}
    , sum({{field}}) as {{field}}
    {% endfor %}
from shifts
left join assignments
    on shifts.shift_schedule_est @> assignments.assigned_at_est
    and shifts.user_id = assignments.assigned_user_id
where shifts.location_name = 'Virtual Care Platform'
    and shifts.start_date_est < current_date
group by 1,2
