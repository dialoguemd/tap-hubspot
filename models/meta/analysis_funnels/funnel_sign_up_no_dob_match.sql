{% set events = [
		'patientapp_screen_signup_dob_aliased',
		'patientapp_screen_signup_email_no_dob_aliased',
		'patientapp_screen_new_profile',
		'patientapp_screen_consent',
		'patientapp_create_account_success'
	]
%}
{% set id_for_aggregation = 'user_id' %}

{{ funnel_analysis(events, id_for_aggregation) }}
