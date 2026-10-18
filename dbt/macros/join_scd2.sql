{#
  Une una dimensión SCD2 con la versión vigente en una fecha:  VALID_FROM <= as_of < VALID_TO.
    dim     modelo de la dimensión
    alias   alias de la dimensión en la consulta
    hk      columna hash key de la dimensión
    key     expresión del hecho que la identifica (p. ej. l.PART_HK)
    as_of   fecha con la que se resuelve la versión (en los hechos, el LOAD_DATETIME del registro)
#}
{% macro join_scd2(dim, alias, hk, key, as_of) -%}
join {{ ref(dim) }} {{ alias }}
    on {{ alias }}.{{ hk }} = {{ key }}
    and {{ as_of }} >= {{ alias }}.VALID_FROM and {{ as_of }} < {{ alias }}.VALID_TO
{%- endmacro %}
