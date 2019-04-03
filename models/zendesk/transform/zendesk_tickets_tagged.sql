with tickets as (
		select * from {{ ref('zendesk_tickets') }}
	)

	, tags as (
		select * from {{ ref('zendesk_tech_support_tags') }}
	)

    , tickets_unnested as (
        select ticket_id
            , unnest(
                string_to_array(tags, ', ')
                ) as tags
        from tickets
    )

    , tickets_tagged as (
        select ticket_id
            , tags
            , tag_type
        from tickets_unnested
        left join tags using (tags)
    )

select ticket_id
    , max(tags) filter(where tag_type = 'feature') as feature_tag
    , max(tags) filter(where tag_type = 'operating_system') as os_tag
    , max(tags) filter(where tag_type = 'platform') as platform_tag
from tickets_tagged
group by 1
