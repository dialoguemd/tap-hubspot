select id, date, file_number, t, a, age, gender, cc, cc_nlp, language
	, d_s, question_count, invalid, worst_response_time
	, worst_response_time_sec, worst_response_time_question, completion_rate
	, thumbs_up, thumbs_down, q1, q1_completed, q2, q2_completed, q3
	, q3_completed, print_timestamp, night_delay
	, collective_prescription, includes_collective_prescription , dx_1, dx_2
	, dx_3, dx_4, dx_5, dxa_t, ctas, dx__md_1, dx_md_2, dx_md_3, score_md, md
	, sheet_ouctcome, additional_notes
	, extract('hour' from date::timestamp) as date_hour
	, case
		when extract('hour' from date::timestamp) < 8 then 'N'
		when extract('hour' from date::timestamp) < 16 then 'J'
		else 'S'
	end as quart_de_travail_arrivee
	, completion_rate = 100 as is_completed
	, case when cc is null then null else print_delay / 60.0 end as print_delay
	, extract(epoch from duration) as duration
from {{ ref('data_dxa_chum_from_dxa') }}
where cc is not null
