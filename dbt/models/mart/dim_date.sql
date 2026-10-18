{# Calendario desde la fecha mínima a la máxima presente en el vault (pedido, envío, compromiso, recepción). #}
with dates as (
    select O_ORDERDATE as d from {{ ref('sat_order') }}
    union all select L_SHIPDATE from {{ ref('sat_lnk_lineitem') }}
    union all select L_COMMITDATE from {{ ref('sat_lnk_lineitem') }}
    union all select L_RECEIPTDATE from {{ ref('sat_lnk_lineitem') }}
),

bounds as (
    select min(d) as lo, max(d) as hi from dates
),

calendar as (
    select dateadd(day, row_number() over (order by seq4()) - 1, b.lo) as DATE_DAY, b.hi
    from bounds b, table(generator(rowcount => 3660))
)

select
    to_number(to_char(DATE_DAY, 'YYYYMMDD')) as DATE_KEY,
    DATE_DAY,
    year(DATE_DAY)                           as YEAR,
    quarter(DATE_DAY)                        as QUARTER,
    month(DATE_DAY)                          as MONTH,
    monthname(DATE_DAY)                      as MONTH_NAME,
    dayofweekiso(DATE_DAY)                   as DAY_OF_WEEK,
    dayname(DATE_DAY)                        as DAY_NAME,
    dayofweekiso(DATE_DAY) in (6, 7)         as IS_WEEKEND
from calendar
where DATE_DAY <= hi
