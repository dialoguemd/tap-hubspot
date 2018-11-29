{{ config(materialized='view') }}

select * from countdown.tracks
