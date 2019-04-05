with
	episodes as (
		select * from {{ ref('episodes_with_contracts') }}
	)

	, issue_types as (
		select * from {{ ref('medops_issue_type_labels') }}
	)

	, episodes_agg as (
		select episodes.organization_id || ' - ' || episodes.organization_name
				as organization_name_id
			, episodes.issue_type
			, issue_types.label_en
			, issue_types.label_fr
			, count(episodes.*)
		from episodes
		left join issue_types
			using (issue_type)
		group by 1,2,3,4
	)

	, ranked as (
		select organization_name_id
			{% for lang in ['en', 'fr'] %}
			, nth_value(label_{{lang}}, 1)
				over (partition by organization_name_id order by count desc)
				as issue_type_first_most_common_{{lang}}
			, nth_value(label_{{lang}}, 2)
				over (partition by organization_name_id order by count desc)
				as issue_type_second_most_common_{{lang}}
			, nth_value(label_{{lang}}, 3)
				over (partition by organization_name_id order by count desc)
				as issue_type_third_most_common_{{lang}}
			{% endfor %}
		from episodes_agg
		where issue_type <> 'other'
	)

select organization_name_id
	{% for lang in ['en', 'fr'] %}
	, min(issue_type_first_most_common_{{lang}}) as issue_type_first_most_common_{{lang}}
	, min(issue_type_second_most_common_{{lang}}) as issue_type_second_most_common_{{lang}}
	, min(issue_type_third_most_common_{{lang}}) as issue_type_third_most_common_{{lang}}
	{% endfor %}
from ranked
group by 1
