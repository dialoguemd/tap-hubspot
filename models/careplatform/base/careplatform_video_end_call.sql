select *
	, timezone('America/Montreal',timestamp) as timestamp_est
	, case
		when path like '/consult/%' then split_part(path, '/', 4)
		else null
		end as episode_id
from careplatform.video_call_call_ended
