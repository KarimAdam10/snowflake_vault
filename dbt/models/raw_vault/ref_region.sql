{# Generado por dbt/scripts/generate_models.py. No editar a mano. #}
{{ automate_dv.ref_table(src_pk='R_REGIONKEY',
   src_extra_columns=['R_NAME', 'R_COMMENT'],
   src_ldts='LOAD_DATETIME',
   src_source='RECORD_SOURCE',
   source_model='stg_region') }}
