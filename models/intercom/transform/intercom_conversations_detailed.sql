with conversations as (
        select * from {{ ref('intercom_conversations') }}
    )

    , conversation_parts as (
        select * from {{ ref('intercom_conversation_parts') }}
    )

    , users as (
        select * from {{ ref('intercom_users') }}
    )

select conversations.conversation_id
    , users.email
    , users.user_id
    , conversations.created_at as conversation_started
    , min(conversation_parts.created_at) filter(where author_type = 'user')
        as user_first_message
    , min(conversation_parts.created_at) filter(where author_type = 'admin')
        as admin_first_message
from conversations
left join conversation_parts
    using (conversation_id)
left join users
    using (intercom_user_id)
group by 1,2,3,4
