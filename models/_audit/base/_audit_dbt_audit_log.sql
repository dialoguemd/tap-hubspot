-- This base model is schema-dependent
-- It audits the deployment speed of this.schema

select * from {{this.schema}}_audit.dbt_audit_log
