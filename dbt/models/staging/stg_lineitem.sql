{# Generado por dbt/scripts/generate_models.py. No editar a mano. #}
{%- set yaml_metadata -%}
source_model: {landing: LND_LINEITEM}
derived_columns: {RECORD_SOURCE: _SOURCE_FILE, LOAD_DATETIME: 'LAST_DAY(TO_DATE(_PERIOD || ''01'', ''YYYYMMDD''))'}
hashed_columns:
  ORDER_HK: L_ORDERKEY
  PART_HK: L_PARTKEY
  SUPPLIER_HK: L_SUPPKEY
  LINEITEM_HK: [L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER]
  LINEITEM_HASHDIFF:
    is_hashdiff: true
    columns: [L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE,
      L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT]
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ automate_dv.stage(include_source_columns=true,
                     source_model=metadata_dict['source_model'],
                     derived_columns=metadata_dict['derived_columns'],
                     hashed_columns=metadata_dict['hashed_columns']) }}
