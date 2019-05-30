-- During the outage (DIA-7486), all user id properties where sent as a single user
-- This script allows us to set the right user id for the period affected

-- create a table with the actual user_id to use
create table analytics.mm_user_mapping as (
    select mm_user_id
        , count(distinct user_id) as user_count
        , max(user_id) filter(where user_id <> '28595') as user_id
    from analytics.messaging_posts_all_time
    group by 1
    having count(distinct user_id) > 1
)
;
-- update posts table with correct user_id
update messaging.posts
set user_id = (
    select mm_user_mapping.user_id
    from analytics.mm_user_mapping
    where posts.mm_user_id = mm_user_mapping.mm_user_id
)
where posts.user_id = '28595'
        and date_trunc('day', posts.timestamp) = '2019-05-23'
;
