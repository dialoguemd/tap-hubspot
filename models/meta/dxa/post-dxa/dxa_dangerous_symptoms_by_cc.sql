select cc_code
	, lower(symptom) as symptom
from {{ ref('data_dxa_dangerous_symptoms_by_cc') }}
