{# Generado por dbt/scripts/generate_models.py. No editar a mano. #}
{{ automate_dv.link(src_pk='PARTSUPP_HK',
   src_fk=['PART_HK', 'SUPPLIER_HK'],
   src_ldts='LOAD_DATETIME',
   src_source='RECORD_SOURCE',
   source_model='stg_partsupp') }}
