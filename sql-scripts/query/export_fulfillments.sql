--SELECT * EXCLUDE(fulfillmentCreatedAtIsoMonthOnly) FROM definition_fulfillments;
--SELECT * FROM definition_fulfillments;

SELECT 
date as orderCreatedAtDateCorrectTimezone,
legacyResourceId,
totalQuantity,
orderId,
orderName,
orderCreatedAt,
fulfillmentCreatedAt,
fulfillmentCostMatrixFiltered,
packagingBaseCostPerOrder,
packagingCostPerAdditionalProduct,
fulFillmentBaseCostPerOrder,
fulFillmentCostPerAdditionalProduct,
packagingCostFinalDecision,
fulfillmentCostFinalDecision,
storehero_fulfillmentandpackagingcostcombined


FROM definition_fulfillments
Order BY orderCreatedAtDateCorrectTimezone DESC
;