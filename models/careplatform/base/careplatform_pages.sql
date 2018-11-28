
{{ config(materialized='table') }}

select * from careplatform.pages
where
{% if target.name == 'dev' %}
     timestamp > current_timestamp - interval '1 months'
{% endif %}
