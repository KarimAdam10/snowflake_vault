{# Dimensión SCD2 de pieza: una fila por versión de sat_part. #}
{{ scd2_dimension(
    satellite='sat_part', hub='hub_part', hk='PART_HK', sk='PART_SK',
    business_key=['P_PARTKEY'],
    attributes=['P_NAME', 'P_MFGR', 'P_BRAND', 'P_TYPE', 'P_SIZE', 'P_CONTAINER', 'P_RETAILPRICE', 'P_COMMENT']) }}
