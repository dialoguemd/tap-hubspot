{%
	set specializations = [
		['Family Physician', 'gp'],
		['Nurse Practitioner', 'np'],
		['Psychologist', 'psy'],
		['Nutritionist', 'nutr'],
		['Psychotherapist', 'psy_therapist']]
%}

with
	video_consultations as (
		select * from {{ ref('videos_detailed') }}
	)

select episode_id
	, min(started_at) as first_video_consultation_started_at
	, count(distinct date_day_est) as video_consultation_count
	, count(distinct date_day_est) > 0 as includes_video_consultation
	, sum(video_length) as video_consultation_length

	{% for spec, acronym in specializations %}
	, count(distinct date_day_est)
		filter (where main_specialization = '{{spec}}')
		> 0 as includes_video_consultation_{{acronym}}
	, count(distinct date_day_est)
		filter (where main_specialization = '{{spec}}')
		as video_consultation_{{acronym}}_count
	, sum(video_length)
		filter (where main_specialization = '{{spec}}')
		as video_consultation_{{acronym}}_length
	{% endfor %}
from video_consultations
where main_specialization in
	(
		{% for spec, acronym in specializations %}
		'{{spec}}' {% if not loop.last %} , {% endif %}
		{% endfor %}
	)
group by 1
