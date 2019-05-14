{{ config(materialized='table') }}

{{ alias_model('patientapp_aliases', 'patientapp_screen_signup_group_id') }}
