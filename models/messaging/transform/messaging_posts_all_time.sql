with messaging_posts as (
       select * from {{ ref('messaging_posts') }}
   )

   , test_users as (
      select * from {{ ref('test_users') }}
   )

   , mm_posts as (
      select * from {{ ref('mm_posts') }}
   )

   , unioned as (
      select *
      from messaging_posts

      union all

      select *
      from mm_posts
   )

select *
from unioned
left join test_users using (user_id)
where test_users.user_id is null
