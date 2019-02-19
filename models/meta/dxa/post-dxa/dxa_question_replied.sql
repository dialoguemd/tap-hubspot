with replies as (
		select * from {{ ref('countdown_question_replied') }}
	)

select episode_id
	, question_tid
	, qnaire_tid
	,lower(
		replace(
			replace(
				replace(question_name, 'iQR_', '')
			, ' ', '')
		, '.', '')) as question_name
	, lower(
		replace(
			replace(reply_labels,'["', ''),
		 '"]', '')) as reply_value
from replies
where qnaire_name = 'dxa'
