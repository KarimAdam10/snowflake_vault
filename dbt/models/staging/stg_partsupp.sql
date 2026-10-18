{# Generado por dbt/scripts/generate_models.py. No editar a mano. #}
{%- set yaml_metadata -%}
source_model: {landing: LND_PARTSUPP}
derived_columns: {RECORD_SOURCE: _SOURCE_FILE, LOAD_DATETIME: 'LAST_DAY(TO_DATE(_PERIOD || ''01'', ''YYYYMMDD''))'}
hashed_columns:
  PART_HK: PS_PARTKEY
  SUPPLIER_HK: PS_SUPPKEY
  PARTSUPP_HK: [PS_PARTKEY, PS_SUPPKEY]
  PARTSUPP_HASHDIFF:
    is_hashdiff: true
    columns: [PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT]
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     hashed_columns=metadata_dict['hashed_columns']) }}
