select
--from raw orders
{{ dbt_utils.surrogate_key(['o.orderid', 'c.customerid', 'p.productid']) }} as sk_orders,
o.orderid,
o.orderdate,
o.shipdate,
o.shipmode,
o.ordersellingprice - o.ordercostprice as orderprofit,
o.ordercostprice,
o.ordersellingprice,
-- from raw customer
c.customername,
c.segment,
c.country,
c.customerid,
--from raw product
p.category,
p.subcategory2 as subcategory,
p.subcategory as productname,
p.productid,
{{markup('ordersellingprice','ordercostprice')}} as markup,
d.delivery_team
from {{ref('raw_order')}} o
left join {{ref('raw_customer')}} c
on o.customerid = c.customerid
left join {{ref('raw_product')}} p 
on o.productid = p.productid
left join {{ref('delivery_team')}} d
on d.shipmode = o.shipmode