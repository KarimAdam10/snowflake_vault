{# Generado por dbt/scripts/generate_models.py. No editar a mano. #}
{{ automate_dv.sat(src_pk='PARTSUPP_HK',
   src_hashdiff='PARTSUPP_HASHDIFF',
   src_payload=['PS_AVAILQTY', 'PS_SUPPLYCOST', 'PS_COMMENT'],
   src_ldts='LOAD_DATETIME',
   src_source='RECORD_SOURCE',
   source_model='stg_partsupp') }}
