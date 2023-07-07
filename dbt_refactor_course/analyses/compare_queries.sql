{% set old_etl_relation=ref('legacy_customer_orders') %}
{% set new_etl_relation=ref('fact_customer_orders') %}

{{ audit_helper.compare_relations(
    a_relation = old_etl_relation, 
    b_relation = new_etl_relation,
    primary_key = order_id
)}}