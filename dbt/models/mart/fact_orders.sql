{#
  Hecho de pedidos: una fila por pedido (cabecera). Aquí vive el importe total del pedido, que no puede ir
  en fact_lineitem porque se repetiría en cada línea y sumaría de más.
  Dimensiones SCD2: cada pedido ve la versión vigente en el LOAD_DATETIME con que se cargó.
#}
with order_attrs as (
    select * from {{ ref('sat_order') }}
    qualify row_number() over (partition by ORDER_HK order by LOAD_DATETIME desc) = 1
)

select
    od.ORDER_SK,
    c.CUSTOMER_SK,
    to_number(to_char(a.O_ORDERDATE, 'YYYYMMDD')) as ORDER_DATE_KEY,
    o.O_ORDERKEY                                  as ORDER_KEY,
    a.O_TOTALPRICE                                as TOTAL_PRICE,
    o.LOAD_DATETIME,
    o.ORDER_HK
from {{ ref('hub_order') }} o
join order_attrs a on a.ORDER_HK = o.ORDER_HK
join {{ ref('lnk_order_customer') }} oc on oc.ORDER_HK = o.ORDER_HK
{{ join_scd2('dim_order', 'od', 'ORDER_HK', 'o.ORDER_HK', 'o.LOAD_DATETIME') }}
{{ join_scd2('dim_customer', 'c', 'CUSTOMER_HK', 'oc.CUSTOMER_HK', 'o.LOAD_DATETIME') }}
