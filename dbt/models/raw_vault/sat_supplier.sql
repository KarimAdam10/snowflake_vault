{# Generado por dbt/scripts/generate_models.py. No editar a mano. #}
{{ automate_dv.sat(src_pk='SUPPLIER_HK',
   src_hashdiff='SUPPLIER_HASHDIFF',
   src_payload=['S_NAME', 'S_ADDRESS', 'S_NATIONKEY', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'],
   src_ldts='LOAD_DATETIME',
   src_source='RECORD_SOURCE',
   source_model='stg_supplier') }}
