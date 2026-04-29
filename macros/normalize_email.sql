{% macro normalize_email(email_column) %}
    lower(trim({{ email_column }}))
{% endmacro %}
