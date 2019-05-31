{% set events = [
		'patientapp_screen_signup_dob_aliased',
		'patientapp_screen_signup_email_dob_aliased',
		'patientapp_screen_info_warning_need_help_aliased',
	]
%}
{% set id_for_aggregation = 'user_id' %}

{{ funnel_analysis(events, id_for_aggregation) }}
