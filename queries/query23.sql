-- query23
WITH /*tcpds_query23*/ frequent_ss_items 
     AS (SELECT Substr(i_item_desc, 1, 30) itemdesc,
                i_item_sk                  item_sk,
                d_date                     solddate,
                Count(*)                   cnt
         FROM   store_sales,
                date_dim,
                item
         WHERE  ss_sold_date_sk = d_date_sk
                AND ss_item_sk = i_item_sk
                AND d_year IN ( 1998, 1998 + 1, 1998 + 2, 1998 + 3 )
         GROUP  BY 1,
                   i_item_sk,
                   d_date
         HAVING Count(*) > 4),
     max_store_sales
     AS (SELECT Max(csales) tpcds_cmax
         FROM   (SELECT c_customer_sk,
                        Sum(ss_quantity * ss_sales_price) csales
                 FROM   store_sales,
                        customer,
                        date_dim
                 WHERE  ss_customer_sk = c_customer_sk
                        AND ss_sold_date_sk = d_date_sk
                        AND d_year IN ( 1998, 1998 + 1, 1998 + 2, 1998 + 3 )
                 GROUP  BY c_customer_sk)),
     best_ss_customer
     AS (SELECT c_customer_sk,
                Sum(ss_quantity * ss_sales_price) ssales
         FROM   store_sales,
                customer
         WHERE  ss_customer_sk = c_customer_sk
         GROUP  BY c_customer_sk
         HAVING Sum(ss_quantity * ss_sales_price) >
                ( 95 / 100.0 ) * (SELECT *
                                  FROM   max_store_sales))
SELECT Sum(sales)
FROM   (SELECT cs_quantity * cs_list_price sales
        FROM   catalog_sales,
               date_dim
        WHERE  d_year = 1998
               AND d_moy = 6
               AND cs_sold_date_sk = d_date_sk
               AND cs_item_sk IN (SELECT item_sk
                                  FROM   frequent_ss_items)
               AND cs_bill_customer_sk IN (SELECT c_customer_sk
                                           FROM   best_ss_customer)
        UNION ALL
        SELECT ws_quantity * ws_list_price sales
        FROM   web_sales,
               date_dim
        WHERE  d_year = 1998
               AND d_moy = 6
               AND ws_sold_date_sk = d_date_sk
               AND ws_item_sk IN (SELECT item_sk
                                  FROM   frequent_ss_items)
               AND ws_bill_customer_sk IN (SELECT c_customer_sk
                                           FROM   best_ss_customer)) LIMIT 100;
