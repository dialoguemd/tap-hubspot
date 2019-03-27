with
    episodes as (
        select * from {{ ref('episodes') }}
    )

    , posts as (
        select * from {{ ref('messaging_posts_all_time') }}
    )

    , q_ask as (
        select * from {{ ref('countdown_question_asked') }}
    )

    , q_reply as (
        select * from {{ ref('countdown_question_replied') }}
    )

    , q_start as (
        select * from {{ ref('countdown_qnaire_completed') }}
    )

    , q_finish as (
        select * from {{ ref('countdown_qnaire_started') }}
    )

    , q_pause as (
        select * from {{ ref('countdown_qnaire_paused') }}
    )

    , answers as (
        select q_ask.qnaire_name
          , q_ask.timestamp
          , q_ask.qnaire_tid
          , q_ask.question_name
          , case when q_ask.question_name = 'episode_subject' and q_reply.reply_labels LIKE '%myself%' 
                    then 'Myself' 
                when q_ask.question_name = 'episode_subject' and q_reply.reply_labels LIKE '%someone_else%'
                    then 'Someone Else'
                when q_ask.question_name = 'episode_subject' and q_reply.reply_labels is null and count(q_pause.qnaire_tid) > 0
                    then 'Free Text Message'
                when q_ask.question_name = 'episode_subject' and q_reply.reply_labels is null
                    then null
                when q_ask.question_name = 'episode_subject' then 'A Dependent'
                else null end as episode_subject
          , case when q_ask.question_name = 'chat_topic' and q_reply.reply_labels LIKE '%feeling_sick%' 
                    then 'Feeling Sick'
                 when q_ask.question_name = 'chat_topic' and q_reply.reply_labels LIKE '%other%'
                    then 'Other'
                 when q_ask.question_name = 'chat_topic' and q_reply.reply_labels LIKE '%pain_or_injury%'
                    then 'Pain or Injury'
                 when q_ask.question_name = 'chat_topic' and q_reply.reply_labels LIKE '%prescription%'
                    then 'Prescription' 
                 when q_ask.question_name = 'chat_topic' and q_reply.reply_labels LIKE '%referral%'
                    then 'Referral'
                 when q_ask.question_name = 'chat_topic' and q_reply.reply_labels LIKE '%mental_health%'
                    then 'Mental Health'
                 when q_ask.question_name = 'chat_topic' and q_reply.reply_labels LIKE '%sexual_health%'
                    then 'Sexual Health'
                 when q_ask.question_name = 'chat_topic' and q_reply.reply_labels LIKE '%dietary%'
                    then 'Dietary'
                 when q_ask.question_name = 'chat_topic' and q_reply.reply_labels LIKE '%chronic_condition%'   
                    then 'Chronic Condition' 
                 when q_ask.question_name = 'chat_topic' and q_reply.reply_labels is null and count(q_pause.qnaire_tid) > 0
                    then 'Free Text Message'
                 else null end as chat_topic
            , episodes.url_zorro
            , first_value(fu_qnaire.qnaire) over (partition by q_ask.qnaire_tid order by fu_qnaire.timestamp) as fu_qnaire
            , count(posts.post_id) as post_qnaire_posts
        from q_ask
        left join episodes
            using (episode_id)
        left join q_reply
          using (qnaire_tid, question_name)
        left join q_start
            using (qnaire_tid)
        left join q_pause
            using (qnaire_tid)
        left join q_finish
            using (qnaire_tid)
        left join q_start as fu_qnaire 
            on q_ask.user_id = fu_qnaire.user_id
            and tstzrange(q_ask.timestamp, q_ask.timestamp + interval '10 minutes') @> fu_qnaire.timestamp  
        left join posts 
            on q_ask.user_id = posts.user_id
            and tstzrange(q_finish.timestamp, q_finish.timestamp + interval '5 minutes') @> posts.created_at  
        where q_ask.qnaire_name = 'top_level_greeting'
            and q_ask.question_name in ('chat_topic', 'episode_subject')
            and episodes.created_at > '5/1/2018'
        group by q_ask.qnaire_name
            , q_ask.qnaire_tid
            , q_ask.timestamp
            , q_ask.question_name
            , q_reply.reply_labels
            , episodes.url_zorro
            , fu_qnaire.qnaire
            , fu_qnaire.timestamp
    ) 

select
    qnaire_name,
    qnaire_tid,
    url_zorro,
    min(timestamp) as timestamp,
    case when max(episode_subject) is null then '[Unanswered]' else max(episode_subject) end as episode_subject,
    case when max(episode_subject) in ('Someone Else', 'Free Text Message') then '[End of Questions]' 
        when max(episode_subject) is null then '[End of Questions]' 
        when max(chat_topic) is null then '[Unanswered]' 
        else max(chat_topic) end  as chat_topic,
    case when max(chat_topic) in ('Someone Else', 'Free Text Message') or max(episode_subject) is null 
            or max(chat_topic) is null then '[End of Questions]'
        when max(fu_qnaire) is null and max(post_qnaire_posts) > 0 then 'Free Text Message'
        else max(fu_qnaire) end as follow_up,
    'Chat Topic' as question
from answers
group by 1,2,3

union  

select
    qnaire_name,
    qnaire_tid,
    url_zorro,
    min(timestamp) as timestamp,
    case when max(episode_subject) is null then '[Unanswered]' else max(episode_subject) end as episode_subject,
    case when max(episode_subject) in ('Someone Else', 'Free Text Message') then '[End of Questions]' 
        when max(episode_subject) is null then '[End of Questions]' 
        when max(chat_topic) is null then '[Unanswered]'
        else max(chat_topic) end  as chat_topic,
    case when max(chat_topic) in ('Someone Else', 'Free Text Message') or max(episode_subject) is null 
            or max(chat_topic) is null then '[End of Questions]'
        when max(fu_qnaire) is null and max(post_qnaire_posts) > 0 then 'Free Text Message'
        else max(fu_qnaire) end as follow_up,
    'Episode Subject' as question
from answers
group by 1,2,3