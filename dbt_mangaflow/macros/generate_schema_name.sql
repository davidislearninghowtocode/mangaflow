{% macro generate_schema_name(custom_schema_name, node) -%}
    {# 
       Nếu có cấu hình custom schema (như bronze, gold trong dbt_project.yml), 
       dbt sẽ dùng luôn tên đó thay vì nối thêm schema mặc định.
    #}
    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema }}
    {%- endif -%}

{%- endmacro %}