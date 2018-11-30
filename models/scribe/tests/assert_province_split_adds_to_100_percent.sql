{{ config(materialized='ephemeral') }}

with
	province_split as (
		select * from {{ ref('scribe_organization_province_split') }}
	)

select *
	, round(qc_perc + on_perc + roc_perc, 4) as total_should_be_one
from province_split
where round(qc_perc + on_perc + roc_perc, 4) <> 1
