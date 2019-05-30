{%
    set facts = [
        'median_age',
        'invited_employee_count',
        'signed_up_employee_rate',
        'signed_up_employee_count',
        'signed_up_family_member_count',
        'activated_employee_count',
        'activated_family_member_count',
        'survey_count_cum',
        'survey_sum_cum',
        'total_consults'
    ]
%}

{% set dimensions = ['date_day', 'account_name'] %}

{{ historical_equivalency_sum_test(facts, dimensions, 'client_dashboard_daily', 0.05) }}
