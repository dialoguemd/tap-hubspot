with choozle as (
		select * from {{ ref( 'funnelio_choozle' ) }}
	)

	, facebook as (
		select * from {{ ref( 'funnelio_facebook' ) }}
	)

	, google as (
		select * from {{ ref( 'funnelio_google' ) }}
	)

	, google_search_console as (
		select * from {{ ref( 'funnelio_google_search_console' ) }}
	)

	, linkedin as (
		select * from {{ ref( 'funnelio_linkedin' ) }}
	)

	, linkedin_organic as (
		select * from {{ ref( 'funnelio_linkedin_organic' ) }}
	)

	, other as (
		select * from {{ ref( 'funnelio_other' ) }}
	)

	, twitter as (
		select * from {{ ref( 'funnelio_twitter' ) }}
	)

	, unioned as (
		select * from choozle
		union all
		select * from facebook
		union all
		select * from google
		union all
		select * from google_search_console
		union all
		select * from linkedin
		union all
		select * from linkedin_organic
		union all
		select * from other
		union all
		select * from twitter
	)

select date
	, platform
	, channel
	, cost
	, currency
	, clicks
	, impressions
	, case when impressions = 0 then 0 
		else clicks / impressions::float
		end as ctr
	, case when clicks = 0 then 0 
		else cost / clicks::float
		end as cpc
	, case when impressions = 0 then 0 
		else cost / impressions * 1000::float 
		end as cpm
from unioned