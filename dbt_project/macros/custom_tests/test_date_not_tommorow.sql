{% test test_date_not_tommorow(model, column_name)%}
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} >= NOW()
{% endtest %}