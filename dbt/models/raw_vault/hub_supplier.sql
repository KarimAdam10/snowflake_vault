{# Generado por dbt/scripts/generate_models.py. No editar a mano. #}
{{ automate_dv.hub(src_pk='SUPPLIER_HK',
   src_nk='S_SUPPKEY',
   src_ldts='LOAD_DATETIME',
   src_source='RECORD_SOURCE',
   source_model='stg_supplier') }}
