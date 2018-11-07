select id::text as user_id
	, case
		when main_specialization is null
		then 'N/A'
		when main_specialization = 'Medical Assistant' then 'Care Coordinator'
		when main_specialization = 'Nurse' then 'Nurse Clinician'
		else main_specialization
	end as main_specialization
from coredata.practitioner
