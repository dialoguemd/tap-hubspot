with
	videos as (
		select * from {{ ref('videos_detailed') }}
	)

	, null_fraction as (
		select date_trunc('week', date_day_est) as week
			, count(*) filter (where video_length is null)
				/ count(*)::float as fraction
		from videos
		group by 1
	)

select week
	, fraction
from null_fraction
where null_fraction.fraction > 0.04
