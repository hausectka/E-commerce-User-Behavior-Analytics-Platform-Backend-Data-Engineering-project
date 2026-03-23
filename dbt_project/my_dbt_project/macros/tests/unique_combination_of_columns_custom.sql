{% test unique_combination_of_columns_custom(model, combination_of_columns) %}

WITH row_numbered AS (
  SELECT
    t.*,
    COUNT(*) OVER (
      PARTITION BY {% for col in combination_of_columns %}
        t.{{ col }}{% if not loop.last %}, {% endif %}
      {% endfor %}
    ) AS dup_count
  FROM {{ model }} AS t
)

SELECT *
FROM row_numbered
WHERE dup_count > 1

{% endtest %}