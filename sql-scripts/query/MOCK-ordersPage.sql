
CREATE OR REPLACE TABLE api_ordersResponse AS 

SELECT 
'9999-99-99' AS date,
'1234' AS customer,
9999.99 AS paid,
9999.99 AS discount,
9999.99 AS shipping_charged,
9999.99 AS shipping_cost,
9999.99 AS fulfillment,
9999.99 AS transactions,
9999.99 AS cogs,
9999.99 AS packing_fees,
9999.99 AS gross_profit,
9999.99 AS gpmargin,
999.99 AS tax,
'tagheres' AS tags,
'Paid' AS payment_status,
'EUR' AS currency_code,
'Ireland' AS shipping_country,
'Dublin' AS shipping_city,
'DU' AS shipping_region,
'Kate@storehero.ai' AS customer_email,
'Country' AS fulfillment_status,
'Country' AS status,
'Country' AS orderId,
99.99 AS productCost,
50.55 AS gross_profit_per_order,
'DPD' AS fultfillment_carrier,



--	ERROR: Object of type ndarray is not JSON serializable

  list(json_object(
  
  'id', '123', 
  'product', 'Shampoo', 
  'quantity', 1,
  'paid', 99.99, 
  'discount', 50.55,
  'cogs', 99.99,
  'product_cost', 11.11,
  'total_packing_fees', 22.22,
  'gross_profit', 33.33,
  'gpm', 44.44
  )) ::varchar AS variants;
  
  
 


--type VecklerOrderVariant = {
--  id: string;
--  product: string;
--  quantity: number;
--  paid: number;
--  discount: number;
--  cogs: number;
--  product_cost: number;
--  total_packing_fees: number;
--  gross_profit: number;
--  gpm: number;
--};




SELECT * FROM api_ordersResponse

--type VecklerOrder = {
--  date: string;
--  customer: VecklerOrderCustomer;
--  paid: number;
--  discount: number;
--  shipping_charged: number;
--  shipping_cost: number;
--  fulfillment: number;
--  transactions: number;
--  cogs: number;
--  packing_fees: number;
--  gross_profit: number;
--  gpmargin: number;
--  tax: number;
--  tags: string[];
--  payment_status: string;
--  currency_code: string;
--  shipping_country: string;
--  shipping_city: string;
--  shipping_region: string;
--  billing_country: string;
--  customer_email: string;
--  fulfillment_status: string;
--  status: string;
--  orderId: string;
--  productCost: number;
--  gross_profit_per_order: number;
--  fultfillment_carrier: string;
--  variants: VecklerOrderVariant[];
--};

--type VecklerOrderVariant = {
--  id: string;
--  product: string;
--  quantity: number;
--  paid: number;
--  discount: number;
--  cogs: number;
--  product_cost: number;
--  total_packing_fees: number;
--  gross_profit: number;
--  gpm: number;
--};
--
--type VecklerOrderCustomer = {
--  name: string;
--  sourceName: string;
--};