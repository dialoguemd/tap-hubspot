with
	scores as (
		select * from {{ ref('mental_health_phq9_scores') }}
	)

	, users as (
		select * from {{ ref('scribe_users') }}
	)

	, summaries as (
		select scores.user_id
			, users.gender
			, users.birthday
			-- Score of first PHQ9
			, min(scores.score)
				filter (where scores.rank = 1) as score_first
			-- Score of PHQ9 after 1 month +/- 7 days
			, min(scores.score)
				filter (where scores.days_since_first_phq9 between 23 and 37) as score_month1
			-- Score of PHQ9 after 3 months +/- 14 days
			, min(scores.score)
				filter (where scores.days_since_first_phq9 between 76 and 104) as score_month3
			-- Score of PHQ9 after 6 months +/- 28 days
			, min(scores.score)
				filter (where scores.days_since_first_phq9 between 152 and 208) as score_month6
		from scores
		left join users
			using (user_id)
		group by 1,2,3
	)

select user_id
	, gender
	, birthday
	, score_first
	{% for n in ["1", "3", "6"] %}
	, score_month{{n}}
	, score_month{{n}} is not null as has_score_month{{n}}
	, round(
		(score_first - score_month{{n}})*-1.0 / score_first
		, 2) as delta_month{{n}}
	{% endfor %}
from summaries
