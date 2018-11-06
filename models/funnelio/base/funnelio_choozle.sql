select common_clicks as clicks
    , common_cost/1000::float as cost 
    , currency
    , common_impressions as impressions
    , channel
    , event as platform
    , date_trunc('day', timestamp) as date
from funnelio.choozle
