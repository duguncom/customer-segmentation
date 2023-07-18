-- thi sql is for budget information of customers
select
pf.provider_id,
pf.customer_id,
last_update_at,
category_id,
city_id,
district_id,
is_promotion,
map_lat, map_lng,
image_count, video_count, discount_count, review_count,
lead_count, view_count,
product_started_at, product_ended_at,
lead_read_gap_min,
budget_value,
product_listing_priority,
membership_length,
valid_product_ids,
description_length,
(select price from customer_agreements where customer_id = pf.customer_id and `status`='active' order by id desc limit 1) as current_products_price,
(select created_at from customer_notifications where customer_id = pf.customer_id order by id desc limit 1) as last_touch,
(select created_at from customer_calls where customer_id = pf.customer_id and talk_duration > 20 order by id desc limit 1) as last_call,
(select last_seen_at from customer_users where customer_id = pf.customer_id order by last_seen_at desc limit 1) as last_seen_at,
(select count(id) from customer_notifications where customer_id = pf.customer_id and created_at > pf.product_started_at) as touch_count,
(select count(id) from customer_calls where customer_id = pf.customer_id and created_at > pf.product_started_at and talk_duration > 20) as call_count
from Provider_Filter as pf
where is_promotion = 0