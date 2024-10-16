/* Macro to take in project id as an input and configure the output tabel */
{% macro custom_config(alias, field) %}
  config(
    alias=alias,
    materialized="incremental",
    incremental_strategy="insert_overwrite",
    partition_by={
      "field": field,
      "data_type": "date",
      "granularity": "day"
    }
  )
{% endmacro %}