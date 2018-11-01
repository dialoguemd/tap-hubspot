
select name as organization_name
    , sf_account_c as salesforce_account_id
    , maestro_org_number_c as organization_id
    , active_provinces_c as active_provinces
from salesforce.maestro_orgs
where not is_deleted