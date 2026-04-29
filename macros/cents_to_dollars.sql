{% macro cents_to_dollars(cents_column, precision=2) %}
    round({{ cents_column }} / 100.0, {{ precision }})
{% endmacro %}
