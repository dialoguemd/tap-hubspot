{% set events = [
		'patientapp_screen_signup_dob_aliased',
		'patientapp_screen_signup_group_id_aliased',
		'patientapp_screen_signup_dob_aliased',
	]
%}
{% set id_for_aggregation = 'user_id' %}

{{ funnel_analysis(events, id_for_aggregation) }}
