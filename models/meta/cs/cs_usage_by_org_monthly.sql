with
    usage as (
        select * from {{ ref('cs_usage_by_org_by_fmt_monthly')}}
    )

select date_month
    , organization_name
    , organization_id
    , account_name
    , billing_start_month
    , ( extract(year from age(date_month, billing_start_month))*12 +
        extract(month from age(date_month, billing_start_month))
        ) as months_since_billing_start
	
	-- Jinja loop for family member types
	{% for family_member_type in ["Employee", "Dependent", "Child"] %}

	, coalesce(
		min(invited_count_cum)
        	filter (where family_member_type = '{{family_member_type}}')
        , 0) as {{family_member_type}}_invited_count_cum
    , coalesce(
    	min(invited_count)
        	filter (where family_member_type = '{{family_member_type}}')
        , 0) as {{family_member_type}}_invited_count
    , coalesce(
    	min(signed_up_count_cum)
        	filter (where family_member_type = '{{family_member_type}}')
        , 0) as {{family_member_type}}_signed_up_count_cum
    , coalesce(
    	min(signed_up_count)
        	filter (where family_member_type = '{{family_member_type}}')
        , 0) as {{family_member_type}}_signed_up_count
    , coalesce(
    	min(activated_count_cum)
        	filter (where family_member_type = '{{family_member_type}}')
        , 0) as {{family_member_type}}_activated_count_cum
    , coalesce(
    	min(activated_count)
        	filter (where family_member_type = '{{family_member_type}}')
        , 0) as {{family_member_type}}_activated_count

	{% endfor %}

from usage
group by 1,2,3,4,5,6
