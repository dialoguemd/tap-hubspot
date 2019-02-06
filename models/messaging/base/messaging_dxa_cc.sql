select *
from messaging.dxa_cc
-- remove events that don't have an episode_id
where timestamp > '2018-11-08 15:07:37.677+00'
