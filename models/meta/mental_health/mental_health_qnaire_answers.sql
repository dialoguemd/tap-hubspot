-- Target-dependent config

{% if target.name == 'dev' %}
  {{ config(materialized='view') }}
{% else %}
  {{ config(materialized='table') }}
{% endif %}

-- 

with countdown_question_replied as (
      select * from {{ ref('countdown_question_replied') }}
  )

  , test_users as (
      select * from {{ ref('scribe_test_users') }}
  )

  ,answers as (
  select qnaire_tid
          , user_id
          , episode_id
          , replied_at
          , case
              when question_name like '%q1'
                then 'Little interest or pleasure in doing things'
              when question_name like '%q2'
                then 'Feeling down, depressed, hopeless'
              when question_name like '%q3'
                then 'Feeling nervous, anxious or on edge'
              when question_name like '%q4'
                then 'Not being able to stop or control worrying'
              when question_name like '%q5'
                then 'Unable to control the important things in your life'
              when question_name like '%q6'
                then 'Confident about your ability to handle your personal problems'
              when question_name like '%q7'
                then 'That things were going your way'
              when question_name like '%q8'
                then 'Difficulties were piling up so high that you could not overcome them'
              end as question
          , question_category
          , replace(
              replace(reply_values, '[', '')
              , ']', '')::int as reply_value
      from countdown_question_replied
      where qnaire_name = 'stress'
  )

select answers.qnaire_tid
    , answers.user_id
    , answers.episode_id
    , row_number() over (partition by answers.user_id order by max(replied_at)) as response_rank
    , max(replied_at) as stress_qnaire_completed_at
    , sum(reply_value) filter(where question_category = 'Depression') as depression_score
    , case when max(reply_value)
      filter(where question_category = 'Depression') >= 2
      then true else false end as depression_flag
    , sum(reply_value)
      filter(where question_category = 'Anxiety') as anxiety_score
    , case when sum(reply_value)
      filter(where question_category = 'Anxiety') >= 3
      then true else false end as anxiety_flag
    , sum(reply_value)
      filter(where question_category = 'Stress') as stress_score
    , case
        when sum(reply_value) filter(where question_category = 'Stress') = 0
          then 'No stress'
        when sum(reply_value) filter(where question_category = 'Stress') <= 5
          then 'Low stress'
        when sum(reply_value) filter(where question_category = 'Stress') <= 10
          then 'Moderate stress'
        else 'High stress' end as stress_grouping
from answers
left join test_users
  using (user_id)
where test_users.user_id is null
group by 1,2,3
