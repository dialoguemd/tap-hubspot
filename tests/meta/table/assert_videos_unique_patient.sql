with
	video_started as (
		select * from {{ ref('video_started') }}
	)

select episode_id
    , count(distinct patient_id)
    , string_agg(patient_id, ', ') 
from video_started
where episode_id is not null
group by episode_id
having count(distinct patient_id) > 1
