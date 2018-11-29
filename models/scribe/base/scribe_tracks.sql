{{ config(materialized='view') }}

select * from scribe.tracks
