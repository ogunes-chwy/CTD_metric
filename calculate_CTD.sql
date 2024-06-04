
-- INSERT DATA TO TABLES

CREATE OR REPLACE TEMPORARY TABLE edldb.sc_promise_sandbox.ctd_as_cat_temp AS
SELECT DISTINCT order_order_id AS order_id,
        CASE WHEN o.order_auto_reorder_subscription_first_placed_flag = false 
                    AND o.order_first_auto_reorder_order_placed_flag = false
                    AND b.business_channel_name IN ('AutoReorder') THEN 'Autoship System'      
             WHEN o.order_auto_reorder_subscription_first_placed_flag = false
                    AND b.business_channel_name IN ('Web','iOS App','Android App')
                    AND olm.order_submitted_by_key != '5121' THEN 'Autoship Ship Now'
             ELSE 'Autoship First Time' END AS autoship_subcategory
        FROM edldb.chewybi.order_line_measures olm
        JOIN edldb.chewybi.orders o
                ON olm.order_order_id=o.order_id
        JOIN edldb.chewybi.business_channels b ON
                olm.business_channel_key = b.business_channel_key   
        WHERE o.order_auto_reorder_flag = TRUE 
        AND DATE(order_placed_dttm_est) >= DATEADD(day, -21, CAST(GETDATE() AS date)); 

        

DELETE FROM edldb.sc_promise_sandbox.ctd_metric_data
WHERE order_placed_date >= DATEADD(day, -21, CAST(GETDATE() AS date)); 
COMMIT;


INSERT INTO edldb.sc_promise_sandbox.ctd_metric_data
WITH base AS
(
SELECT CAST(orderid AS integer) AS order_id,
       shipment_tracking_number,
       DATE(chewypromiseddeliverydate) AS delivery_date,
       'pdd' AS delivery_date_type,
       'N/A' AS ffmcenter_name,
       'N/A' AS Business
FROM EDLDB.WIZMO.WIZMO_ORDER pdd_t
LEFT JOIN chewybi.shipment_transactions st 
        on pdd_t.orderid = st.order_id
WHERE shipment_tracking_number IS NULL  
        AND DATE(timeplaced) >= DATEADD(day, -21, CAST(GETDATE() AS date))  
UNION
SELECT CAST(order_id as integer) as order_id,
       shipment_tracking_number,
       CASE 
            WHEN bulk_track_delivery_dttm_est IS NOT NULL 
            THEN DATE(bulk_track_delivery_dttm_est) 
            ELSE shipment_estimated_delivery_date END 
                AS delivery_date,
        CASE 
            WHEN bulk_track_delivery_dttm_est IS NOT NULL 
            THEN 'actual delivery date' 
            ELSE 'edd' END 
                AS delivery_date_type,
        st.ffmcenter_name,
        CASE 
            WHEN fulfillment_center_dropship_flag THEN '4.Dropship'
            WHEN location_display_name ilike '%Pharmacy%' THEN '2.Pharmacy'
            WHEN location_display_name ilike '%Freezer%' THEN '3.Frozen'
            WHEN  shipment_contains_fresh THEN '5.Fresh'
            ELSE '1.Core' END AS Business
FROM chewybi.shipment_transactions st
JOIN chewybi.locations AS b 
    ON st.ffmcenter_id = b.fulfillment_center_id
WHERE DATE(order_placed_dttm_est) >= DATEADD(day, -21, CAST(GETDATE() AS date)) 
)
SELECT DISTINCT b.order_id,    
                CASE 
                    WHEN delivery_date_type!='pdd' THEN b.shipment_tracking_number
                    ELSE CAST(b.order_id AS varchar(50)) END AS shipment_tracking_number,
                delivery_date,
                delivery_date_type,
                b.ffmcenter_name,
                Business,
               DATE(od.order_placed_dttm_est) AS order_placed_date,
               COALESCE(autoship_subcategory, 'Non AS') AS as_label,
               DATE(delivery_date) - DATE(od.order_placed_dttm_est) AS CTD,
               DATE(chewypromiseddeliverydate) - DATE(od.order_placed_dttm_est) AS pdd_CTD,
               DATE(shipment_estimated_delivery_date) - DATE(od.order_placed_dttm_est) AS edd_CTD,
               DATE(bulk_track_delivery_dttm_est) - DATE(od.order_placed_dttm_est) AS actual_CTD,
               CASE 
                    WHEN od.legal_company_description IN ('Retail Canada') THEN 'Canada' 
                    ELSE 'US' END AS Country
FROM base b
LEFT JOIN edldb.chewybi.orders od 
    ON b.order_id = od.order_id
LEFT JOIN edldb.sc_promise_sandbox.ctd_as_cat_temp as_cat 
        ON as_cat.order_id = b.order_id   
LEFT JOIN edldb.chewybi.shipment_transactions st 
    ON b.order_id = st.order_id  
    AND b.shipment_tracking_number = st.shipment_tracking_number
LEFT JOIN EDLDB.WIZMO.WIZMO_ORDER pdd_t 
    ON b.order_id = CAST(pdd_t.orderid AS integer)  
WHERE od.order_status_description != 'Canceled'; 

COMMIT;



DELETE FROM edldb.sc_promise_sandbox.ctd_metric_report
WHERE order_placed_date >= DATEADD(day, -21, CAST(GETDATE() AS date)); 
COMMIT;


INSERT INTO edldb.sc_promise_sandbox.ctd_metric_report
SELECT  order_placed_date,
        Country,
        Business,        
        CASE 
            WHEN as_label = 'Autoship System' 
            THEN 'AS' ELSE 'Non-AS' 
            END AS AS_Label, 
        ffmcenter_name, 
        delivery_date_type,
        sum( case when CTD is not null then CTD end) as Total_CTD,
        count(distinct order_id) as Total_Orders,
        count(distinct shipment_tracking_number) as Total_Packages,
        sum( case when delivery_date_type!='actual delivery date' then 1 else 0 end) as Not_Delivered_Package ,
        sum( case when delivery_date_type='actual delivery date' then 1 else 0  end) as Delivered_Package ,
        sum( case when CTD is null then 1 else 0  end) as "NULL_CTD_Packages" ,
        sum( case when CTD in (0,1) then 1 else 0  end) as "1D_CTD_Packages" ,
        sum( case when CTD in (0,1,2) then 1 else 0  end) as "2D_CTD_Packages" ,
        sum( case when CTD in (0,1,2,3) then 1 else 0  end) as "3D_CTD_Packages" ,
        sum( case when CTD in (0,1,2,3,4) then 1 else 0  end) as "4D_CTD_Packages" ,
        sum( case when CTD in (0,1,2,3,4,5) then 1 else 0  end) as "5D_CTD_Packages" ,
        sum( case when CTD in (0,1,2,3,4,5,6) then 1 else 0  end) as "6D_CTD_Packages" ,
        sum( case when CTD in (0,1,2,3,4,5,6,7) then 1 else 0  end) as "7D_CTD_Packages" ,
        sum( case when CTD in (0,1,2,3,4,5,6,7,8) then 1 else 0  end) as "8D_CTD_Packages" ,
        sum( case when CTD > 8 then 1 else 0  end) as "Morethan_8D_CTD_Packages" ,
        sum( case when CTD > 5 then 1 else 0  end) as "Morethan_5D_CTD_Packages" ,
        sum( case when CTD > 7 then 1 else 0  end) as "Morethan_7D_CTD_Packages" ,
        sum( case when pdd_CTD > 5 then 1 else 0  end) as "Morethan_5D_pdd_CTD_Packages" ,
        sum( case when edd_CTD > 5 then 1 else 0  end) as "Morethan_5D_edd_CTD_Packages" ,
        sum( case when actual_CTD > 5 then 1 else 0  end) as "Morethan_5D_actual_CTD_Packages" 
FROM edldb.sc_promise_sandbox.ctd_metric_data
WHERE order_placed_date >= DATEADD(day, -21, CAST(GETDATE() AS date))
GROUP BY 1,2,3,4,5,6
ORDER BY 1,2,3,4,5,6;

COMMIT;

-- END
