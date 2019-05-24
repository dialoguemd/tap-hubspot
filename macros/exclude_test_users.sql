{% macro exclude_test_users() %}

left join {{ ref('scribe_test_users') }}
    using (user_id)
where scribe_test_users.user_id is null

{% endmacro %}
