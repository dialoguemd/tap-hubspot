
{{
  config({
    "materialized":"incremental",
    "sql_where":"created_at > (select max(created_at) from {{this}})",
    "post-hook": [
       "DROP INDEX IF EXISTS {{ this.schema }}.index_posts_all_time_created",
       "CREATE INDEX IF NOT EXISTS index_posts_all_time_created ON {{ this }}(created_at)"
    ]
  })
}}

with messaging_posts as (
      select * from {{ ref('messaging_posts') }}
      -- Only add incremental jinja to messaging_posts because mm_posts is
      -- now deprecated as of 2018-09-01
      {% if adapter.already_exists(this.schema, this.table) and not flags.FULL_REFRESH %}
         where created_at > (select max(created_at) from {{ this }})
      {% endif %}
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

select post_id
    , post_type
    , user_id
    , mm_user_id
    , user_type
    , episode_id
    , message_length
    , count_question_marks
    , next_appointment
    , is_question
    , is_internal_post
    , mention
    -- Take the max to take the later post and ignore Segment's manipulation
    -- of timestamps; take the later to not interfere also with the incremental
    -- materialization of the model
    , max(created_at) as created_at
from unioned
left join test_users using (user_id)
where test_users.user_id is null
group by 1,2,3,4,5,6,7,8,9,10,11,12
