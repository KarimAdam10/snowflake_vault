{# Generado por dbt/scripts/generate_models.py. No editar a mano. #}
{%- set yaml_metadata -%}
source_model: {landing: LND_ORDERS}
derived_columns: {RECORD_SOURCE: _SOURCE_FILE, LOAD_DATETIME: 'LAST_DAY(TO_DATE(_PERIOD || ''01'', ''YYYYMMDD''))'}
hashed_columns:
  ORDER_HK: O_ORDERKEY
  CUSTOMER_HK: O_CUSTKEY
  ORDER_CUSTOMER_HK: [O_ORDERKEY, O_CUSTKEY]
  ORDERS_HASHDIFF:
    is_hashdiff: true
    columns: [O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT]
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     hashed_columns=metadata_dict['hashed_columns']) }}
