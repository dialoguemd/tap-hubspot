{{ config(materialized='view') }}

select * from usher.tracks
