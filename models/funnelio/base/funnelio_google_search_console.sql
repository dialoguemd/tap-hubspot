select common_clicks as clicks
    , common_cost/1000::float as cost 
    , currency
    , common_impressions as impressions
    , coalesce(channel, 'No search term available')::text as channel
    , event as platform
    , date_trunc('day', timestamp) as date
from funnelio.google_search_console
