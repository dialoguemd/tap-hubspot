select date_jour_arrivee::date
	, quart_de_travail_arrivee
	, coalesce(nombre_episodes, 0) as nombre_episodes
from {{ ref('data_dxa_chum_daily') }}
