select
--from raw orders
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
p.productid
from {{ref('raw_order')}} o
left join {{ref('raw_customer')}} c
on o.customerid = c.customerid
left join {{ref('raw_product')}} p 
on o.productid = p.productid