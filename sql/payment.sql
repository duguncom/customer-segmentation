-- this sql is for the record of payments 
select
payment.id as payment_id,
case when payment.recurring_month is null and amount = 1 then 1 else payment.recurring_month end as recurring_month,
ca.id as customer_agreement_id,
date(payment.created_at) as payment_created_at,
cp.customer_id,
cp.provider_id,
status_id as payment_status_id,
payment_due_date,
payment_date,
amount,
case when payment_due_date >= curdate() then null else
    (case when status_id = 'cancelled' then null else datediff(payment_date,payment_due_date) end) end as payment_delay
FROM customer_payments as cp
INNER JOIN payments as payment on payment.id = cp.payment_id
INNER JOIN customer_agreements as ca on  ca.id = cp.customer_agreement_id
WHERE cp.customer_id NOT IN (24126)
and cp.deleted_at is null
  #and provider.deleted_at is null
  and payment.deleted_at is null
and ca.deleted_at is null
and payment.payment_due_date >= '2019-01-01';