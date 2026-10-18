{#
  Dimensión SCD2 desde un satélite del Raw Vault: una fila por versión de cada clave.
    satellite, hub  modelos del Raw Vault
    hk              hash key (columna común de hub y satélite)
    sk              nombre de la clave subrogada de la dimensión
    business_key    columnas de clave de negocio (del hub)
    attributes      columnas descriptivas del satélite
    nation_column   (opcional) código de nación: añade NATION_NAME y REGION_NAME
  Vigencia: la primera versión vale desde 1900-01-01; cada una vale hasta la LOAD_DATETIME de la siguiente.
#}
{% macro scd2_dimension(satellite, hub, hk, sk, business_key, attributes, nation_column=none) -%}
with versions as (
    select
        s.{{ hk }},
        {%- for c in business_key %}
        h.{{ c }},
        {%- endfor %}
        {%- for c in attributes %}
        s.{{ c }},
        {%- endfor %}
        {%- if nation_column %}
        s.{{ nation_column }},
        {%- endif %}
        s.LOAD_DATETIME,
        row_number() over (partition by s.{{ hk }} order by s.LOAD_DATETIME) as rn,
        lead(s.LOAD_DATETIME) over (partition by s.{{ hk }} order by s.LOAD_DATETIME) as next_load_datetime
    from {{ ref(satellite) }} s
    join {{ ref(hub) }} h on h.{{ hk }} = s.{{ hk }}
),

validity as (
    select
        *,
        case when rn = 1 then to_date('1900-01-01') else LOAD_DATETIME end as valid_from,
        coalesce(next_load_datetime, to_date('9999-12-31')) as valid_to
    from versions
)

select
    {{ dbt_utils.generate_surrogate_key([hk, 'valid_from']) }} as {{ sk }},
    v.{{ hk }},
    {%- for c in business_key %}
    v.{{ c }},
    {%- endfor %}
    {%- for c in attributes %}
    v.{{ c }},
    {%- endfor %}
    {%- if nation_column %}
    n.N_NAME as NATION_NAME,
    r.R_NAME as REGION_NAME,
    {%- endif %}
    v.valid_from as VALID_FROM,
    v.valid_to as VALID_TO,
    (v.next_load_datetime is null) as IS_CURRENT
from validity v
{%- if nation_column %}
left join {{ ref('ref_nation') }} n on n.N_NATIONKEY = v.{{ nation_column }}
left join {{ ref('ref_region') }} r on r.R_REGIONKEY = n.N_REGIONKEY
{%- endif %}
{%- endmacro %}
