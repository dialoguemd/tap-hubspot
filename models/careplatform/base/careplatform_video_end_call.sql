select user_id
	, timestamp
	, {{ to_est() }}
	, coalesce(episode_id,
		case
			when path like '/consult/%' then split_part(path, '/', 4)
			else null
		end) as episode_id
	, patient_id
from careplatform.video_call_call_ended
