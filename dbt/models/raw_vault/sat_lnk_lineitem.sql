{# Generado por dbt/scripts/generate_models.py. No editar a mano. #}
{{ automate_dv.sat(src_pk='LINEITEM_HK',
   src_hashdiff='LINEITEM_HASHDIFF',
   src_payload=['L_QUANTITY', 'L_EXTENDEDPRICE', 'L_DISCOUNT', 'L_TAX', 'L_RETURNFLAG', 'L_LINESTATUS', 'L_SHIPDATE', 'L_COMMITDATE', 'L_RECEIPTDATE', 'L_SHIPINSTRUCT', 'L_SHIPMODE', 'L_COMMENT'],
   src_ldts='LOAD_DATETIME',
   src_source='RECORD_SOURCE',
   source_model='stg_lineitem') }}
