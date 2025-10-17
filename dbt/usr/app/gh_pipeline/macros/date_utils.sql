{% macro subtract_days(date_str, days=7) %}
    to_date('{{ date_str }}', 'YYYY-MM-DD') - INTERVAL '{{ days }} days'
{% endmacro %}