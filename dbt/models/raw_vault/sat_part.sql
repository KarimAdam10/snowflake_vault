{# Generado por dbt/scripts/generate_models.py. No editar a mano. #}
{{ automate_dv.sat(src_pk='PART_HK',
   src_hashdiff='PART_HASHDIFF',
   src_payload=['P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT'],
   src_ldts='LOAD_DATETIME',
   src_source='RECORD_SOURCE',
   source_model='stg_part') }}
