{# Generado por dbt/scripts/generate_models.py. No editar a mano. #}
{%- set yaml_metadata -%}
source_model: {landing: LND_CUSTOMER}
derived_columns: {RECORD_SOURCE: _SOURCE_FILE, LOAD_DATETIME: 'LAST_DAY(TO_DATE(_PERIOD || ''01'', ''YYYYMMDD''))'}
hashed_columns:
  CUSTOMER_HK: C_CUSTKEY
  CUSTOMER_HASHDIFF:
    is_hashdiff: true
    columns: [C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT]
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     hashed_columns=metadata_dict['hashed_columns']) }}
