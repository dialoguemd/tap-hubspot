{%
    set facts = [
        'total_daus',
        'total_active_on_chat',
        'total_active_on_video',
        'employee_invited_count',
        'employee_signed_up_count',
        'employee_activated_count',
        'dependent_invited_count',
        'dependent_signed_up_count',
        'dependent_activated_count',
        'child_invited_count',
        'child_signed_up_count',
        'child_activated_count'
    ]
%}

{% set dimensions = ['date_month', 'account_name'] %}

{{ historical_equivalency_sum_test(facts, dimensions, 'cs_organization_monthly', 0.05) }}
