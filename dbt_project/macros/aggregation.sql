{% macro get_lags(current, previous) %}
    CASE WHEN {{ previous }} = 0 THEN NULL
    ELSE ( {{ current }} - {{ previous }} ) / {{ previous }} * 100.0
    end
  
{% endmacro %}