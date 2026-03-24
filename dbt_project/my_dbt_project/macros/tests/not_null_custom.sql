{% test not_null_custom(model, column_name) %}
  SELECT *
  FROM {{ model }}
  WHERE {{ column_name }} IS NULL
{% endtest %}