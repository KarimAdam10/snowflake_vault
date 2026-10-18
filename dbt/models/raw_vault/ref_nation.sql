{# Generado por dbt/scripts/generate_models.py. No editar a mano. #}
{{ automate_dv.ref_table(src_pk='N_NATIONKEY',
   src_extra_columns=['N_NAME', 'N_REGIONKEY', 'N_COMMENT'],
   src_ldts='LOAD_DATETIME',
   src_source='RECORD_SOURCE',
   source_model='stg_nation') }}
