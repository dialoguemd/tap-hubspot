{% set facts = ['user', 'episode', 'patient'] %}

{% set dimensions = ['date_week', 'chat_type'] %}

{{ historical_equivalency_count_test(facts, dimensions, 'chats', 0.05) }}
