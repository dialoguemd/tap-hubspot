select *
	, split_part(path, '/', 4) as episode_id
from careplatform.create_episode_note_success
