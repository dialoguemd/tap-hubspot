with data as (
        select * from {{ ref('dxa_cc_classifier_data') }}
    )

	, transformed as (
		select *
			, shift_manager_label is not null or
				patient_confirmation_label is not null as dxa_launched
			, cc_name_rank_1 is not null as cc_parsed
			, doctor_label in
				(cc_name_rank_1, cc_name_rank_2, cc_name_rank_3)
				as doctor_label_detected
			, shift_manager_label in
				(cc_name_rank_1, cc_name_rank_2, cc_name_rank_3)
				as shift_manager_label_detected
			, patient_confirmation_label in
				(cc_name_rank_1, cc_name_rank_2, cc_name_rank_3)
				as patient_confirmed
		from data
	)

select date_trunc('day', replied_at) as date_day
	, count(*) filter (where doctor_label_detected)
		as doctor_label_detected_count
	, count(*)
		filter (where shift_manager_label_detected or patient_confirmed)
		as detected_label_used_count
	, count(*) filter (where shift_manager_label_detected)
		as shift_manager_label_detected_count
	, count(*) filter (where patient_confirmed)
		as patient_confirmed_count
	, count(*) filter (where cc_parsed)
		as cc_parsed_count
	, count(*) filter (where dxa_launched)
		as count_dxa_launched
	, count(*) as total_count
from transformed
group by 1
