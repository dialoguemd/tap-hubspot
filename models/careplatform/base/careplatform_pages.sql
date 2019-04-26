
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
			, row_number() over (partition by user_id, timestamp order by timestamp) as rank
		from careplatform.pages
		where user_id is not null
		{% if is_incremental() %}
			and timestamp > (select max(timestamp) from {{ this }})
		{% endif %}
	)

select *
from ranked
where rank = 1
