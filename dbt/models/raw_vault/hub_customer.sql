{# Generado por dbt/scripts/generate_models.py. No editar a mano. #}
{{ automate_dv.hub(src_pk='CUSTOMER_HK',
   src_nk='C_CUSTKEY',
   src_ldts='LOAD_DATETIME',
   src_source='RECORD_SOURCE',
   source_model='stg_customer') }}
