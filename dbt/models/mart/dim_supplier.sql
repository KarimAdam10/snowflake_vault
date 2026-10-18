{# Dimensión SCD2 de proveedor: una fila por versión de sat_supplier, con nación y región. #}
{{ scd2_dimension(
    satellite='sat_supplier', hub='hub_supplier', hk='SUPPLIER_HK', sk='SUPPLIER_SK',
    business_key=['S_SUPPKEY'],
    attributes=['S_NAME', 'S_ADDRESS', 'S_PHONE', 'S_ACCTBAL', 'S_COMMENT'],
    nation_column='S_NATIONKEY') }}
