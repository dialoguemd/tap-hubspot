select user_id
	, dependent_id::text as child_id
	, family_id
	, timestamp
from scribe.child_added
