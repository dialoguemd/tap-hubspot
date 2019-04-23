with
	chum_daily as (
		select * from {{ ref('dxa_chum_daily') }}
	)

	, chum_from_dxa as (
		select * from {{ ref('dxa_chum_from_dxa') }}
	)

	, chum_from_dxa_daily as (
		select date_trunc('day', date::timestamp) as date
			, quart_de_travail_arrivee
			, count(*) as dxa_started
			, count(*) filter(where is_completed) as dxa_completed
			, count(*) filter(where score_md is not null) as md_scored
			, bool_or(md in (
				'Alexandre Tratch'
				, 'Julien Martel'
				, 'Louis Charbonneau'
				)
			) as includes_dxa_md
		from chum_from_dxa
		group by 1,2
	)

select chum_daily.date_jour_arrivee as date
	, chum_daily.quart_de_travail_arrivee
	, chum_daily.date_jour_arrivee ||
		chum_daily.quart_de_travail_arrivee as shift
	, chum_daily.nombre_episodes
	, coalesce(chum_from_dxa_daily.dxa_started, 0)
		as dxa_started
	, coalesce(chum_from_dxa_daily.dxa_completed, 0)
		as dxa_completed
	, coalesce(md_scored, 0) as md_scored
	, 1.0 * coalesce(chum_from_dxa_daily.dxa_completed, 0)
		/ chum_from_dxa_daily.dxa_started as dxa_completion_rate
	, 1.0 * coalesce(chum_from_dxa_daily.dxa_completed, 0)
		/ chum_daily.nombre_episodes as completed_over_episodes
	, includes_dxa_md
from chum_daily
left join chum_from_dxa_daily
	on chum_daily.date_jour_arrivee = chum_from_dxa_daily.date
		and chum_daily.quart_de_travail_arrivee
			= chum_from_dxa_daily.quart_de_travail_arrivee
