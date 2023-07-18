-- this sql is for satisfaction status which is wanted from other teams and not included in our segmentation process
select cn.customer_id,
       cn.provider_id,
       cn.created_at as satisfaction_date,
       c.value,
       c.item_id as value_id
       from customer_notifications as cn
inner join (SELECT
P.customer_id,
date_add(date_add(LAST_DAY(min(PPP.starts_on)),interval 1 DAY),interval -1 MONTH) as min_date
            FROM
            products AS PR
            JOIN product_feature AS PF ON (PR.id = PF.product_id)
            JOIN product_price AS PP ON (PR.id = PP.product_id)
            JOIN provider_product_price AS PPP ON (PP.id = PPP.`product_price_id`)
            JOIN providers AS P ON (P.id = PPP.provider_id)
JOIN city AS L ON (P.city_id = L.id)
JOIN provider_category AS PC ON (P.category_id = PC.id)
            WHERE
            PF.deleted_at IS NULL AND
            PR.deleted_at IS NULL AND
            PPP.deleted_at IS NULL AND
            PPP.status = 1 AND
            P.deleted_at is NULL and
            P.customer_id != 24126 and
            (CURDATE() BETWEEN PPP.starts_on AND PPP.ends_on) AND
            PF.listing_type_id IS NOT NULL
and PR.is_freemium = 0 and PPP.is_trial = 0
group by P.customer_id) as cus on cus.customer_id = cn.customer_id and cn.created_at >= cus.min_date
inner join config as c on cn.satisfaction_status = c.item_id and c.item_type = 'satisfaction_status';