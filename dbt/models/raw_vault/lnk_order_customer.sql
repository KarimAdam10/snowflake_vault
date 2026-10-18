{# Generado por dbt/scripts/generate_models.py. No editar a mano. #}
{{ automate_dv.link(src_pk='ORDER_CUSTOMER_HK',
   src_fk=['ORDER_HK', 'CUSTOMER_HK'],
   src_ldts='LOAD_DATETIME',
   src_source='RECORD_SOURCE',
   source_model='stg_orders') }}
