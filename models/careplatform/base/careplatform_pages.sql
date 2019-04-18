
{{
  config(
    materialized='incremental',
    unique_key='id',
    post_hook=[
       "{{ postgres.index(this, 'id')}}",
    ]
  )
}}

with
	ranked as (
		select *
			, rank() over (partition by user_id, timestamp order by timestamp) as rank
		from careplatform.pages
		{% if is_incremental() %}
		where timestamp > (select max(timestamp) from {{ this }})
		{% endif %}
	)

select *
from ranked
where rank = 1
