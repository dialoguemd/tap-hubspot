{{ config(materialized='view') }}

select * from careplatform.tracks
