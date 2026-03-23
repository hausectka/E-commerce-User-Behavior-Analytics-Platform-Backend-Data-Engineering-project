{% test stock_code_length(model, column_name) %}
  SELECT *
  FROM {{ model }}
  WHERE LENGTH(CAST({{ column_name }} AS STRING)) != 5
{% endtest %}