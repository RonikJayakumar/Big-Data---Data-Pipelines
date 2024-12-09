{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}  {# Use the provided custom schema name directly #}
    {%- else -%}
        {{ target.schema }}  {# Default to target schema if no custom name is provided #}
    {%- endif -%}
{%- endmacro %}
