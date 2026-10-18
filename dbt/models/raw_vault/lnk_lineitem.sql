{# Generado por dbt/scripts/generate_models.py. No editar a mano. #}
{{ automate_dv.link(src_pk='LINEITEM_HK',
   src_fk=['ORDER_HK', 'PART_HK', 'SUPPLIER_HK'],
   src_extra_columns=['L_LINENUMBER'],
   src_ldts='LOAD_DATETIME',
   src_source='RECORD_SOURCE',
   source_model='stg_lineitem') }}
