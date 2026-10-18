{#
  Hecho de ventas: una fila por línea de pedido.
  - Medidas y reglas de negocio: del satélite de la línea y del Business Vault (bsat_lineitem).
  - Dimensiones SCD2: cada línea ve la versión vigente en el LOAD_DATETIME con que se cargó.
  - Atributos degenerados (sin dimensión propia): número de pedido y de línea, modos de envío y banderas.
    Los atributos del pedido están en dim_order.
#}
with line as (
    select * from {{ ref('lnk_lineitem') }}
),

line_attrs as (
    select * from {{ ref('sat_lnk_lineitem') }}
    qualify row_number() over (partition by LINEITEM_HK order by LOAD_DATETIME desc) = 1
),

line_rules as (
    select * from {{ ref('bsat_lineitem') }}
    qualify row_number() over (partition by LINEITEM_HK order by LOAD_DATETIME desc) = 1
),

order_attrs as (
    select * from {{ ref('sat_order') }}
    qualify row_number() over (partition by ORDER_HK order by LOAD_DATETIME desc) = 1
),

orders as (
    select o.ORDER_HK, o.O_ORDERKEY, a.O_ORDERDATE
    from {{ ref('hub_order') }} o
    join order_attrs a on a.ORDER_HK = o.ORDER_HK
)

select
    c.CUSTOMER_SK,
    p.PART_SK,
    s.SUPPLIER_SK,
    od.ORDER_SK,
    to_number(to_char(o.O_ORDERDATE, 'YYYYMMDD'))   as ORDER_DATE_KEY,
    to_number(to_char(a.L_SHIPDATE, 'YYYYMMDD'))    as SHIP_DATE_KEY,
    to_number(to_char(a.L_COMMITDATE, 'YYYYMMDD'))  as COMMIT_DATE_KEY,
    to_number(to_char(a.L_RECEIPTDATE, 'YYYYMMDD')) as RECEIPT_DATE_KEY,
    o.O_ORDERKEY                                    as ORDER_KEY,
    l.L_LINENUMBER                                  as LINE_NUMBER,
    a.L_SHIPMODE                                    as SHIP_MODE,
    a.L_SHIPINSTRUCT                                as SHIP_INSTRUCT,
    a.L_RETURNFLAG                                  as RETURN_FLAG,
    a.L_LINESTATUS                                  as LINE_STATUS,
    a.L_QUANTITY                                    as QUANTITY,
    a.L_EXTENDEDPRICE                               as EXTENDED_PRICE,
    a.L_DISCOUNT                                    as DISCOUNT,
    a.L_TAX                                         as TAX,
    r.NET_PRICE,
    r.CHARGE,
    r.DAYS_IN_TRANSIT,
    r.DAYS_LATE,
    r.IS_LATE,
    l.LOAD_DATETIME,
    l.LINEITEM_HK
from line l
join line_attrs a on a.LINEITEM_HK = l.LINEITEM_HK
join line_rules r on r.LINEITEM_HK = l.LINEITEM_HK
join orders o on o.ORDER_HK = l.ORDER_HK
join {{ ref('lnk_order_customer') }} oc on oc.ORDER_HK = l.ORDER_HK
{{ join_scd2('dim_customer', 'c', 'CUSTOMER_HK', 'oc.CUSTOMER_HK', 'l.LOAD_DATETIME') }}
{{ join_scd2('dim_part', 'p', 'PART_HK', 'l.PART_HK', 'l.LOAD_DATETIME') }}
{{ join_scd2('dim_supplier', 's', 'SUPPLIER_HK', 'l.SUPPLIER_HK', 'l.LOAD_DATETIME') }}
{{ join_scd2('dim_order', 'od', 'ORDER_HK', 'l.ORDER_HK', 'l.LOAD_DATETIME') }}
