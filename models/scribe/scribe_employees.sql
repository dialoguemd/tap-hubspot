-- Employees 

select users.address_country
    , users.address_state
    , users.age
    , users.auth_id
    , users.avatar
    , users.birthday
    , users.company_id
    , users.company_remove
    , users.context_library_name
    , users.context_library_version
    , users.country
    , users.created_at
    , users.description
    , users.email
    , users.family_id
    , users.family_member_type
    , users.first_name
    , users.gender
    , users.id as user_id
    , users.language
    , users.last_name
    , users.organization_id as organization_id
    , users.phone
    , users.residence_province
    , users.segment_object_id
    , users.state
    , users.status
    , users.uuid_ts
    , true as is_employee
    , false as is_child
    , users.id as plan_member_id
from scribe_dev.users
where users.organization_id is not null