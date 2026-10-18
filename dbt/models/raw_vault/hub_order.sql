{# Generado por dbt/scripts/generate_models.py. No editar a mano. #}
{{ automate_dv.hub(src_pk='ORDER_HK',
   src_nk='O_ORDERKEY',
   src_ldts='LOAD_DATETIME',
   src_source='RECORD_SOURCE',
   source_model='stg_orders') }}
