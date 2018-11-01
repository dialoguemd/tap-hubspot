select id as account_id
	, name as account_name
	, coalesce(number_of_employees, number_of_employees_c) as number_of_employees
	, billing_city
	, billing_country
	, billing_postal_code
	, last_activity_date
	, last_viewed_date
	, shipping_state
	, shipping_street
	, annual_revenue
	, billing_street
	, master_record_id
	, owner_id
	, billing_state
	, created_by_id
	, created_date
	, description
	, last_modified_date
	, last_referenced_date
	, fax
	, shipping_postal_code
	, summary_c
	, website
	, phone
	, ownership
	, system_modstamp
	, type
	, account_source
	, industry
	, mrr_c::float as mrr
	, ticker_symbol
	, uuid_ts
	, parent_id
	, rating
	, record_type_id
	, shipping_city
	, is_deleted
	, last_modified_by_id
	, photo_url
	, shipping_country
	, shipping_country_code
	, billing_country_code
	, billing_state_code
	, shipping_state_code
	, dept_source_c as dept_source
	, bs_account_status_c as bs_account_status
	, bs_activity_status_c as bs_activity_status
	, bs_open_opportunities_c as bs_open_opportunities
	, insurance_carrier_c as insurance_carrier
	, preferred_rate_c as preferred_rate
	, average_number_of_monthly_renewals_c as average_number_of_monthly_renewals
	, monthly_price_per_employee_c as monthly_price_per_employee
	, partner_broker_consultant_firm_c as partner_broker_consultant_firm
	, shipping_street_2_c as shipping_street_2
	, street_2_c as street_2
	, unionized_c as unionized
	, billing_date_c as billing_date
	, click_to_dial_click_to_call_skype_c as click_to_dial_click_to_call_skype
	, company_type_c as company_type
	, sdr_c as sdr
	, total_partner_opportunities_mrr_c as total_partner_opportunities_mrr
	, bs_status_icon_c as bs_status_icon
	, client_churn_date_c as client_churn_date
	, crunchbase_c as crunchbase
	, executive_sponsor_c as executive_sponsor
	, owner_not_a_user_c as owner_not_a_user
	, commission_rate_c as commission_rate
	, number_of_brokers_c as number_of_brokers
	, number_of_clients_c as number_of_clients
	, click_to_dial_click_to_call_phone_c as click_to_dial_click_to_call_phone
	, zisf_zoom_lastupdated_c as zisf_zoom_lastupdated
	, active_c as active
	, activity_icon_c as activity_icon
	, total_closed_lives_c as total_closed_lives
	, linked_in_c as linked_in
	, partner_broker_consultant_individual_c as partner_broker_consultant_individual
	, referred_by_c as referred_by
	, target_account_c as target_account
	, zisf_zoom_info_industry_c as zisf_zoom_info_industry
	, bs_lost_opportunities_c as bs_lost_opportunities
	, client_start_date_c as client_start_date
	, relationship_c as relationship
	, relationship_status_c as relationship_status
	, bs_won_opportunities_c as bs_won_opportunities
	, account_executive_c as account_executive
	, number_of_lives_c as number_of_lives
	, of_open_cases_c as of_open_cases
	, partner_insurance_carrier_c as partner_insurance_carrier
	, bs_account_po_1_c as bs_account_po_1
	, lead_source_partner_c as lead_source_partner
	, account_id_external_c as account_id_external
	, ae_c as ae
	, bs_days_since_last_activity_c as bs_days_since_last_activity
	, cirrusadv_created_by_cirrus_insight_c as cirrusadv_created_by_cirrus_insight
	, data_quality_description_c as data_quality_description
	, tier_c as tier
	, prospecting_status_c as prospecting_status
	, days_in_current_status_c as days_in_current_status
	, last_prospect_status_change_date_c as last_prospect_status_change_date
	, rnw_date_c as rnw_date
	, churn_reason_details_c as churn_reason_details
	, churn_reason_c as churn_reason
	, latest_billing_start_date_c as latest_billing_start_date
	, cs_account_manager_c as cs_account_manager
	, zisf_zoom_info_complete_status_c as zisf_zoom_info_complete_status
	, zisf_zoom_clean_status_c as zisf_zoom_clean_status
	, broker_advisor_c as broker_advisor
	, current_mrr_c as current_mrr
	, current_paying_employees_c as current_paying_employees
	, lf_visit_count_c as lf_visit_count
from salesforce.accounts
where not is_deleted
