with reply as (
        select * from {{ ref('countdown_question_replied') }}
    )

select episode_id
	, timestamp
	, reply_value as descript
	, user_id
	, row_number()
		over (partition by episode_id order by timestamp desc)
		as rank_desc
from reply
where reply.question_name = 'symptoms'
    and reply.reply_value is not null
    and reply.qnaire_name in
        ('feeling_sick', 'chronic', 'ask_symptoms', 'chief_complaint')
