{% macro test_record_count(model, n_records) %}

select count(*)
from (
    select

      count(*)

    from {{ model }}

    having count(*) <> {{ n_records }}

) validation_errors

{% endmacro %}
