{# Generado por dbt/scripts/generate_models.py. No editar a mano. #}
select s.R_REGIONKEY
from {{ ref('stg_region') }} s
left join {{ ref('ref_region') }} r on r.R_REGIONKEY = s.R_REGIONKEY
where r.R_REGIONKEY is null
   or r.R_NAME is distinct from s.R_NAME
   or r.R_COMMENT is distinct from s.R_COMMENT
