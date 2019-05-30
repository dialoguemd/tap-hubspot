{% set facts = ['account', 'organization', 'patient'] %}

{% set dimensions = ['date_day', 'family_member_type'] %}

{{ historical_equivalency_count_test(facts, dimensions, 'active_users', 0.05) }}
