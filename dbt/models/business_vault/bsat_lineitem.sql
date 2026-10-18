{#
  Business Vault: satélite calculado sobre lnk_lineitem (una fila por versión de sat_lnk_lineitem).
  Reglas de negocio de la línea; son vistas porque dependen solo de datos ya versionados en el Raw Vault.
#}
select
    LINEITEM_HK,
    LOAD_DATETIME,
    RECORD_SOURCE,
    L_EXTENDEDPRICE * (1 - L_DISCOUNT)                      as NET_PRICE,
    L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)        as CHARGE,
    datediff(day, L_SHIPDATE, L_RECEIPTDATE)                as DAYS_IN_TRANSIT,
    greatest(datediff(day, L_COMMITDATE, L_RECEIPTDATE), 0) as DAYS_LATE,
    L_RECEIPTDATE > L_COMMITDATE                            as IS_LATE
from {{ ref('sat_lnk_lineitem') }}
