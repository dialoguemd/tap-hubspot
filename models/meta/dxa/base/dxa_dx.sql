select *
	{% for n in ['1', '2', '3', '4', '5'] %}
	, split_part(dx_{{n}}, '|', 1) as dx_label_{{n}}
	, split_part(dx_{{n}}, '|', 2) as dx_score_{{n}}
    {% endfor %}
from {{ ref('data_dxa_dx') }}
