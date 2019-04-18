select *
	, timezone('America/Montreal', timestamp) as timestamp_est
from telephone_master.call_started
