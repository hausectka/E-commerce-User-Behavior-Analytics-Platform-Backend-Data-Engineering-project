{% test accepted_values_custom_from_model(model, column_name, accepted_model, accepted_column, use_source=false) %}

{% if use_source %}
  {% set query %}
    SELECT DISTINCT {{ accepted_column }}
    FROM {{ source('raw', accepted_model) }}
  {% endset %}
{% else %}
  {% set query %}
    SELECT DISTINCT {{ accepted_column }}
    FROM {{ ref(accepted_model) }}
  {% endset %}
{% endif %}

{% set results = run_query(query) %}
{% if execute %}
  {% set allowed_values = results.columns[0].values() %}
{% else %}
  {% set allowed_values = [] %}
{% endif %}

SELECT *
FROM {{ model }}
WHERE {{ column_name }} NOT IN (
  {% for val in allowed_values %}
    '{{ val }}'{% if not loop.last %}, {% endif %}
  {% endfor %}
)

{% endtest %}