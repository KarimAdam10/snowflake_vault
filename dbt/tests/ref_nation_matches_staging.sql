{# Generado por dbt/scripts/generate_models.py. No editar a mano. #}
select s.N_NATIONKEY
from {{ ref('stg_nation') }} s
left join {{ ref('ref_nation') }} r on r.N_NATIONKEY = s.N_NATIONKEY
where r.N_NATIONKEY is null
   or r.N_NAME is distinct from s.N_NAME
   or r.N_REGIONKEY is distinct from s.N_REGIONKEY
   or r.N_COMMENT is distinct from s.N_COMMENT
