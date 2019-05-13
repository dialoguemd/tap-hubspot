{% set events = [
		'patientapp_screen_signup_dob_aliased',
		'patientapp_screen_signup_employee_id_aliased',
		'patientapp_screen_new_profile',
		'patientapp_screen_consent'
	]
%}
{% set id_for_aggregation = 'user_id' %}

{{ funnel_analysis(events, id_for_aggregation) }}
