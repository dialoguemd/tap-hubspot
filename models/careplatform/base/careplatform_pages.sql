
{{ config(materialized='table') }}

select * from careplatform.pages
{% if target.name == 'dev' %}
where timestamp > current_timestamp - interval '2 months'
{% endif %}
