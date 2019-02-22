with replies as (
        select * from {{ ref('dxa_questions') }}
    )

select episode_id
	, qnaire_tid
	, question_tid
	, left(reply_value, position('__' in reply_value)-1) as question_name
    , right(reply_value,
    	(length(reply_value) - position('__' in reply_value) - 1))
    	as reply
from replies
where reply_value like '%iqr%'
