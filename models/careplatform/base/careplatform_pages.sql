
{{ config(materialized='table') }}

select * from careplatform.pages
{% if target.name == 'dev' %}
where timestamp > current_timestamp - interval '1 months'
{% endif %}
