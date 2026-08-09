{#
  Use the model's configured `schema` verbatim (bronze/silver/gold) instead of
  dbt's default behavior of prefixing the target schema. This keeps the
  medallion layers cleanly separated within the Unity Catalog `analytics`
  catalog.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
