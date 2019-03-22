select *
	, split_part(path, '/', 4) as episode_id
from careplatform.retract_message_request
