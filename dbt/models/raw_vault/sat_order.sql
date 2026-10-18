{# Generado por dbt/scripts/generate_models.py. No editar a mano. #}
{{ automate_dv.sat(src_pk='ORDER_HK',
   src_hashdiff='ORDERS_HASHDIFF',
   src_payload=['O_ORDERSTATUS', 'O_TOTALPRICE', 'O_ORDERDATE', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT'],
   src_ldts='LOAD_DATETIME',
   src_source='RECORD_SOURCE',
   source_model='stg_orders') }}
