with
    videos_detailed as (
        select * from {{ ref('videos_detailed') }}
    )

    , episodes as (
        select * from {{ ref('episodes') }}
    )

select videos_detailed.issue_type
    , avg(video_length)
    , count(*) as video_count
    , count(*)  filter(where dxa_completed_at is not null)
        as video_with_dxa_count
    , avg(video_length) filter(where dxa_completed_at is not null)
        as avg_length_with_dxa
    , avg(video_length) filter(where dxa_completed_at is null)
        as avg_length_without_dxa
    , median(video_length) filter(where dxa_completed_at is not null)
        as median_length_with_dxa
    , median(video_length) filter(where dxa_completed_at is null)
        as median_length_without_dxa
    , max(video_length) filter(where dxa_completed_at is not null)
        as max_length_with_dxa
    , max(video_length) filter(where dxa_completed_at is null)
        as max_length_without_dxa
from videos_detailed
inner join episodes
    using (episode_id)
where
    -- Dr. Lalla: MD with the highest number of videos, confirmed using DXA
    -- summary when available
    careplatform_user_id = '63011'
    -- after release of DXA
    and started_at > '2018-11-15'
    -- issues when DXA is used
    and videos_detailed.issue_type in ('derm', 'ent', 'gyn', 'gu', 'gi')
group by 1
