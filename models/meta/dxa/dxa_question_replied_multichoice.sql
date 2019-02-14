with replies as (
        select * from {{ ref('dxa_question_replied') }}
    )

select episode_id
	, qnaire_tid
	, question_tid
	-- Pain intensity
	, case when reply_value like '%iQR_doul_intense%'
		then replace(reply_value, 'iQR_doul_intense__', '')::integer
		else null end as pain_intensity
	-- General location
	, case when reply_value = 'iQR_doul_endroits*__N'
			then 'no_location'
		when reply_value like '%endroit%'
			and reply_value not like '%endroit2%'
		then right(reply_value,
			(length(reply_value)-position('__DL' in reply_value)-3))
		else null end as pain_location
	-- Location detailed
	, case when reply_value like '%endroit2%'
		then right(reply_value,
			(length(reply_value)-position('__DL' in reply_value)-3))
		else null end as pain_location_detailed
from replies
