{#
  Dimensión SCD2 de pedido: atributos de la cabecera. O_TOTALPRICE (medida) y O_ORDERDATE (clave de
  fecha) no van aquí, sino en fact_orders.
#}
{{ scd2_dimension(
    satellite='sat_order', hub='hub_order', hk='ORDER_HK', sk='ORDER_SK',
    business_key=['O_ORDERKEY'],
    attributes=['O_ORDERSTATUS', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT']) }}
