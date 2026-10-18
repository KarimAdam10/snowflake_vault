{# Dimensión SCD2 de cliente: una fila por versión de sat_customer, con nación y región. #}
{{ scd2_dimension(
    satellite='sat_customer', hub='hub_customer', hk='CUSTOMER_HK', sk='CUSTOMER_SK',
    business_key=['C_CUSTKEY'],
    attributes=['C_NAME', 'C_ADDRESS', 'C_PHONE', 'C_ACCTBAL', 'C_MKTSEGMENT', 'C_COMMENT'],
    nation_column='C_NATIONKEY') }}
