select common_clicks as clicks
    , common_cost/1000::float as cost 
    , currency
    , common_impressions as impressions
    , 'Organic'::text as channel
    , event as platform
    , date_trunc('day', timestamp) as date
from funnelio.linked_in_organic
