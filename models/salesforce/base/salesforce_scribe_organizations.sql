select sf_account_c as account_id
    , maestro_org_number_c as organization_id
    , name as organization_name
    , active_provinces_c as active_provinces
from salesforce.maestro_orgs
where not is_deleted
