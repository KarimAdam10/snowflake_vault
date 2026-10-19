{#
  Dimensión SCD2 de pedido: atributos de la cabecera. O_ORDERDATE (clave de fecha) va en fact_lineitem;
  O_TOTALPRICE no se expone: es la suma del cargo (CHARGE) de las líneas del pedido.
#}
{{ scd2_dimension(
    satellite='sat_order', hub='hub_order', hk='ORDER_HK', sk='ORDER_SK',
    business_key=['O_ORDERKEY'],
    attributes=['O_ORDERSTATUS', 'O_ORDERPRIORITY', 'O_CLERK', 'O_SHIPPRIORITY', 'O_COMMENT']) }}
