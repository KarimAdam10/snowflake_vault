{# Generado por dbt/scripts/generate_models.py. No editar a mano. #}
{{ automate_dv.hub(src_pk='PART_HK',
   src_nk='P_PARTKEY',
   src_ldts='LOAD_DATETIME',
   src_source='RECORD_SOURCE',
   source_model='stg_part') }}
