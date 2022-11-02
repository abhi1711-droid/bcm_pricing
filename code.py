# -*- coding: utf-8 -*-
"""
Created on Tue Mar 15 13:15:29 2022

@author: Rishabh Jain
"""


import pandas as pd
import numpy as np
#import pymysql as sql
import pygsheets
import psycopg2 as sql
import datetime

#%%
start = datetime.datetime.now() + pd.Timedelta(hours=5, minutes=30)
with open('/home/abhishekkumar2/bcm/bcm_pricing/output_code.txt', 'a') as f:
    f.write('{} - code started \n'.format(start))
db = sql.connect(host = 'sp-prod-redshift-cluster-1.ckpnz6k3j114.ap-south-1.redshift.amazonaws.com', 
                 user = 'rishabh_jain_read_user', 
                 password ='QXd9VCqvUQN8yAgE',
                 database = 'production',
                 port = 5439)
cursor = db.cursor()
#%%
start = datetime.datetime.now()


#%%
print("query started 0")
cursor.execute(f'''SELECT 
             
              sell_lead_id, reg_no, 0.82*actual_refurb_cost
               
               from 
               
               (SELECT sell_lead_id, registration_no AS reg_no, SUM(total_refurb_cost) AS actual_refurb_cost
FROM(

select sell_lead_id,registration_no,car_city,refurb_category,invoice_created_at
-- ,invoice_workshop_name,invoice_no,sum(part_fixed_count) part_fixed_count
-- ,sum(actual_part_cost) total_part_cost,sum(actual_labour_cost) as actual_labour_cost
,sum(actual_refurb_cost) total_refurb_cost
-- ,job_card_id,advisor_name,max(in_date) workshop_in_date,max(out_date) last_workshop_out_date
from (select sell_lead_id,registration_no,car_city,invoice_created_at,
CASE WHEN refurb_category IS NULL THEN 'REFURB'
WHEN refurb_category ='N_A' THEN 'REFURB'
ELSE refurb_category end as refurb_category,
invoice_workshop_name
,invoice_no,job_card_id,advisor_name,in_date,out_date,count(distinct microcarpartname) part_fixed_count
,sum(microjobrow_part_count*microjobrow_part_cost) actual_part_cost
,sum(microjobrow_labour_cost)actual_labour_cost
,sum(microjobrow_part_count*microjobrow_part_cost + microjobrow_labour_cost)actual_refurb_cost
,DENSE_RANK() OVER(PARTITION BY invoice_no, registration_no ORDER BY advisor_name) AS rank_
from(select
sell_lead_id,registration_no,car_city,make,model,variant,transmission,fuel_type,make_year,mileage,color,car_created_at,
refurb_car_created_at,refurb_car_updated_at,inspector_assigned_time,refurb_car_status,inspector_name
, case when refurb_category is null then invoice_refurb_category else refurb_category end as refurb_category

,advisor_name,dw2.workshop_name,baytype,jobcard_created_at,jobcard_created_by,jobcard_updated_at,jobcard_status,created_at_by_worker
,first_sent_for_approval_at,first_approved_at,work_started_at,first_sent_for_verification_at,i.in_date,i.in_mileage,i.out_date,i.out_mileage

,mjr.micro_job_row_id,microjobrow_created_at,microjobrow_updated_at,microcarpartname,microfix,microjobrow_part_cost,microjobrow_labour_cost,hsn,hsn_rate,sac,sac_rate
,microjobrow_brand,microjobrow_part_count,microjobrow_approval_status,microjobrow_verification,microjobrow_priority,bi.before_image,ai.after_image

,payment_mode,invoice_created_at,invoice_no,trading_partner,invoice_workshop_name,i.job_card_id -- , jl.joblist_verdict

from

(select job_card_id,payment_mode,out_date,customer_id,vendor_id,created_at + interval '330 minutes' invoice_created_at
,updated_at + interval '330 minutes' invoice_updated_at,invoice_no,in_mileage,discount,in_date,out_mileage,refurb_car_id,id invoice_id,car_id
from sp_refurb_external.sp_refurb_dashboard_invoice )i
left join (select job_card_id,user_id from sp_refurb_external.sp_refurb_dashboard_jobcardusermapping) as dju on dju.job_card_id = i.job_card_id
left join (select id,(lower(first_name)||' '|| lower(last_name)) as advisor_name from sp_refurb_external.sp_refurb_auth_user) as au1 on dju.user_id=au1.id

left join (select id, name trading_partner from sp_refurb_external.sp_refurb_customer_customer)cc on cc.id =i.customer_id
left join (select id, name invoice_workshop_name from sp_refurb_external.sp_refurb_dashboard_workshop)iw on iw.id = i.vendor_id
left join (select invoice_id,refurb_category invoice_refurb_category from sp_refurb_external.sp_refurb_dashboard_invoicemetadata)imd on imd.invoice_id = i.invoice_id

full join (select id micro_job_row_id,created_at + interval '330 minutes' microjobrow_created_at
,updated_at + interval '330 minutes' microjobrow_updated_at,part_cost microjobrow_part_cost,labour_cost microjobrow_labour_cost, brand microjobrow_brand
,part_count microjobrow_part_count,approval_status microjobrow_approval_status,verification microjobrow_verification,priority microjobrow_priority,fix_id
,hsn_id,part_id,sac_id,invoice_id,job_card_id,job_row_id
from sp_refurb_external.sp_refurb_dashboard_microjobrow ) mjr
on i.invoice_id = mjr.invoice_id

left join (select id, name microcarpartname from sp_refurb_external.sp_refurb_dashboard_microcarpart) mcp on mcp.id = mjr.part_id
left join (select id,gst hsn_rate,code hsn from sp_refurb_external.sp_refurb_dashboard_taxcode)hsn on hsn.id = mjr.hsn_id
left join (select id,gst sac_rate,code sac from sp_refurb_external.sp_refurb_dashboard_taxcode)sac on sac.id = mjr.sac_id
left join (select id,name microfix from sp_refurb_external.sp_refurb_dashboard_microfix)mf on mf.id = mjr.fix_id
left join (select context_id mjr_id,count(file_upload_id) before_image from sp_refurb_external.sp_refurb_file_filemodelrelation fr where context_type_id=24 and category_id =2 group by context_id)bi on bi.mjr_id =mjr.micro_job_row_id
left join (select context_id mjr_id,count(file_upload_id) after_image from sp_refurb_external.sp_refurb_file_filemodelrelation fr where context_type_id=24 and category_id =3 group by context_id)ai on ai.mjr_id =mjr.micro_job_row_id

left join (select id car_id,created_at + interval '330 minutes' as car_created_at,registration_no,make,model,variant
,transmission,fuel_type,year make_year,mileage,color,comment,hub_location,proc_category,object_id sell_lead_id

from sp_refurb_external.sp_refurb_dashboard_car )dc on dc.car_id = i.car_id

left join (select id,created_at + interval '330 minutes' refurb_car_created_at,updated_at + interval '330 minutes' as refurb_car_updated_at
,assigned_date + interval '330 minutes' as inspector_assigned_time,status refurb_car_status
,car_id,inspector_assigned_id,customer_id,refurb_category,workshop_id from sp_refurb_external.sp_refurb_dashboard_refurbcar) rc on rc.id = i.refurb_car_id

-- LEFt JOIN (select car_id,inspection_status as joblist_verdict from sp_refurb_external.sp_refurb_dashboard_joblist WHERE inspection_status IN ('BCM', 'N/A')) jl on jl.car_id=rc.id

left join
(select id,name,
case
when name like '%Spinny Workshop Delhi NCR%' then 'Delhi NCR'
when name like '%Spinny Workshop Bangalore%' then 'Bangalore'
when name like '%Spinny Workshop Hyderabad%' then 'Hyderabad'
when name like '%Spinny Workshop Pune%' then 'Pune'
when name like '%Spinny Workshop Mumbai%' then 'Mumbai'
when name like '%Spinny Workshop Ahmedabad%' then 'Ahmedabad'
when name like '%Spinny Workshop Chennai%' then 'Chennai'
when name like '%Spinny Workshop Kolkata%' then 'Kolkata'
else 'Others'
end as car_city
from sp_refurb_external.sp_refurb_dashboard_workshop order by id asc )w
on w.id=rc.workshop_id
left join (select id,(first_name||' '||last_name) as inspector_name from sp_refurb_external.sp_refurb_auth_user)au on au.id = inspector_assigned_id

full join (select id job_card_id,created_at + interval '330 minutes' jobcard_created_at,updated_at + interval '330 minutes' jobcard_updated_at
,status jobcard_status,created_by_id jobcard_created_by_id,workshop_id
,created_at_by_worker +interval '330 minutes' created_at_by_worker,first_sent_for_approval_at +  interval '330 minutes' first_sent_for_approval_at
,first_approved_at +interval '330 minutes' first_approved_at,first_sent_for_verification_at + interval '330 minutes' first_sent_for_verification_at
,in_date + interval '330 minutes' in_date ,in_mileage,is_approved_for_workshop,out_date + interval '330 minutes'  out_date,out_mileage,parent_job_card_id
,work_started_at + interval'330 minutes' work_started_at,bay_type_id,car_id
from sp_refurb_external.sp_refurb_dashboard_jobcard ) djc on djc.car_id = rc.id and djc.job_card_id = i.job_card_id
and djc.job_card_id = mjr.job_card_id
left join (select id,name workshop_name from sp_refurb_external.sp_refurb_dashboard_workshop)dw2 on dw2.id = djc.workshop_id
left join (select id, name baytype from sp_refurb_external.sp_refurb_dashboard_workshopbaytype) dbt on dbt.id = djc.bay_type_id
left join (select id,(first_name||' '||last_name) as jobcard_created_by from sp_refurb_external.sp_refurb_auth_user)jccb on jccb.id = jobcard_created_by_id

)sub1
where invoice_created_at>='2021-01-01' -- AND joblist_verdict IN ('BCM','N/A')
and invoice_workshop_name is not null
group by invoice_no,job_card_id,advisor_name,invoice_workshop_name,sell_lead_id,registration_no,car_city,invoice_created_at,refurb_category,in_date,out_date
)t
WHERE refurb_category IN ('REFURB', 'PRE_SALE_REWORK' , 'PRE_SALE_MAINTENANCE','PRE_SALE_CAR_RECEIVING_AT_HUB', 'PRE_SALE','PRE_SALE_CAR_DELIVERY')
-- and registration_no = 'mh20by8325'
AND rank_ = 1
group by invoice_no,sell_lead_id,registration_no,car_city,
invoice_created_at,refurb_category
order by invoice_created_at
) AS a GROUP BY sell_lead_id, registration_no
                                

)as D''')

refurb_df = pd.DataFrame(cursor, columns=('sell_lead_id', 'reg_no', 'actual_refurb_cost'))   






#print(df)                  
#%%
print("query started 1")
cursor.execute(
    f'''
    select 
cp.id deal_id

,Case when st.description = 'Token Collected' then 'Pending' 
when st.description = 'Deal Cancelled' then 'Cancelled' 
when st.description = 'Delivery Done' then 'Delivered' 
when st.description = 'Car Returned' then 'Delivered+3 Day Return' 
ELSE st.description end as deal_status 

,cp.sell_lead_id
,registration_no

,CASE WHEN ah.display_name LIKE '%Vision 1 Mall, Wakad, Pune%' THEN 'Vision One Mall'
WHEN ah.display_name LIKE '%Indirapuram Habitat Centre, Ghaziabad%' THEN 'Indirapuram'
WHEN ah.display_name LIKE '%Royal Meenakshi Mall, Bannerghatta%' THEN 'Meenakshi Mall Bangalore' 
WHEN ah.display_name LIKE '%Mani Square Mall, E. M. Bypass Road, Kolkata%' THEN 'Kolkata Mall'
WHEN ah.display_name LIKE '%Shree Balaji Agora Mall, Motera, Ahmedabad%' THEN 'Agora Mall'
WHEN ah.display_name LIKE '%Truebil Car Hub, Marina Mall, Egattur, Chennai%' THEN 'Marina Mall'
WHEN ah.display_name LIKE '%Mantri Square Mall, Malleshwara, Bangalore%' THEN 'Mantri Mall Bangalore'
WHEN ah.display_name LIKE '%Universal Trade Towers, Sector 49, Sohna Road, Gurgaon%' THEN 'Sohna Road'
WHEN ah.display_name LIKE '%DLF MyPad, Vibhuti Khand, Gomti Nagar, Lucknow%' THEN 'Lucknow Central'
WHEN ah.display_name LIKE '%Neelkanth Business Park, Vidyavihar, Mumbai%' THEN 'Truebil Mumbai'
WHEN ah.display_name LIKe '%Sandhya Elite, Narsing Nanakramguda Service Rd, Financial District, Hyderabad%' THEN 'Truebil Cars, Forum Mall, Hyderabad'
WHEN ah.display_name LIKE '%Best Business Park, Netaji Subhash Place, Delhi%' THEN 'NSP'
WHEN ah.display_name LIKE '%Truebil Car Hub, TDI Centre, Jasola, Delhi%' THEN 'Jasola'
WHEN ah.display_name LIKE '%Truebil Car Hub, Global Business Park, Chandigarh%' THEN 'Truebil Cars Hub Global Business Park Chandigarh'
WHEN ah.display_name LIKE '%Truebil Car Hub, Club Aquaria - Borivali, Mumbai%' THEN 'Borivali'
WHEN ah.display_name LIKE '%Truebil Car Hub, Rcube Monad Mall, Rajouri%' THEN 'Rajouri'
WHEN ah.display_name LIKE '%Truebil Car Hub, Jalsa Mall, Jaipur%' THEN 'Jaipur'
WHEN ah.display_name LIKE '%Truebil Car Hub, Nexus Shanti Niketan Mall - Whitefield Rd, Bangalore%' THEN 'Whitefield'
ELSE ah.display_name END AS deal_hub

,mm.display_name AS make 
,mmo.display_name model
,mv.display_name variant
,lp.color
,make_year
,mileage
,lp.fuel_type
,no_of_owners
,cp.buy_lead_id
,s.sub_sub_source_f
-- ,sub_sub_source
,cd.name buyer_name
,cd.contact_number buyer_number
,inside_sales
,sau2.full_name hub_sales

,pc.price procurement_price

,listing_price
,cp.final_amount sell_price
,4800 AS paper_transfer_cost
,1500 AS warranty_cost
,lrv.insurance_validity_date AS insurance_validity_date

,SUM(pr.amount) AS payment_amount
,pp.token_amount
-- ,insurance_renewal
-- ,insurance_renewal_amount
,date(ll.added_on + interval '5.5 hour') as car_live_date
,date(cp.created_time + interval '5.5 hour') AS token_date


,date(target_delivery_time + interval '5.5 hour') AS EDD1

-- ,case when loan_id is null then 0 else 1 end as loan
,CASE WHEN cp.loan_facilitation = 0 THEN 'No'
    WHEN cp.loan_facilitation = 1 THEN 'Yes' END AS loan_interest
,loan_vendor
,loan_flow_status
,date(login_date) AS login_date
,NULLIF(date(approved_date),date(rejected_date)) approved_rejected_date
,net_disbursed_amount
,CASE WHEN cp.loan_facilitation = 1 AND cp.self_loan = 1 THEN 'Yes' 
    WHEN cp.loan_facilitation = 1 AND cp.self_loan = 0 THEN 'No'
    ELSE NULL END AS Self_loan_interest
,cpr.status AS refund_status



,date(cp.delivery_time + interval '5.5 hour') AS delivery_time

,date(deal_cancellation_time + interval '5.5 hour') AS deal_cancellation_time
,cp.cancellation_reason
,date(car_return_time + interval '5.5 hour') AS car_return_time
,car_return_reason
-- ,DATE(pc.time_create + interval '5.5 hour') AS proc_date






from 

(SELECT * FROM sp_web_external.sp_web_buy_lead_carpurchase
    WHERE id IN
    (
    SELECT prev_deal As id FROM
    (
    SELECT cp.id, cp.buy_lead_id, cp.created_time, s.description,
    LAG(s.description) OVER (PARTITION BY cp.buy_lead_id ORDER BY cp.created_time) AS prev_status,
    LAG(cp.id) OVER (PARTITION BY cp.buy_lead_id ORDER BY cp.created_time) AS prev_deal,
    LAG(cp.created_time) OVER (PARTITION BY cp.buy_lead_id ORDER BY cp.created_time) AS prev_deal_date
    FROM (SELECT * FROM sp_web_external.sp_web_buy_lead_carpurchase) cp
    JOIN sp_web_external.sp_web_status_status s ON s.id = cp.status_id
    JOIN (SELECT * FROM sp_web_external.sp_web_buy_lead_buylead WHERE category = 'bcm') AS bl ON bl.id = cp.buy_lead_id
    WHERE s.description NOT IN ('deal requested', 'deal expired')) as k WHERE prev_status = 'Delivery Done'
    AND EXTRACT(MONTH FROM prev_deal_date) = EXTRACT(MONTH FROM created_time)
    
    
    UNION
    
    SELECT MAX(cp.id) AS id 
                    FROM sp_web_external.sp_web_buy_lead_carpurchase cp
                    JOIN sp_web_external.sp_web_status_status s ON s.id = cp.status_id
                    WHERE DATE(created_time + interval '5.5 hour') >= '2021-10-01'
                    AND s.description NOT IN ('deal requested', 'deal expired')
                    GROUP BY buy_lead_id, EXTRACT(MONTH FROM created_time)
    ))  cp
 
left join sp_web_external.sp_web_status_status st on st.id = cp.status_id
left join sp_web_external.sp_web_listing_lead l on l.id = cp.sell_lead_id
left join sp_web_external.sp_web_listing_leadprofile lp on lp.id = l.profile_id
left join sp_web_external.sp_web_listing_listing ll on ll.lead_id = l.id
left join sp_web_external.sp_web_address_hub ah on ah.id = cp.deal_hub_id
left join sp_web_external.sp_web_make_make mm on mm.id = lp.make_id
left join sp_web_external.sp_web_make_model mmo on mmo.id = lp.model_id
left join sp_web_external.sp_web_make_variant mv on mv.id = lp.variant_id
left join sp_web_external.sp_web_buy_lead_buylead bl on bl.id = cp.buy_lead_id

   
left join (
SELECT a.*, sau.full_name AS inside_sales
FROM
(select wu.context_id,MAX(wu.finished_by_id) AS finished_by_id from sp_web_external.sp_web_workflow_usertask wu
where wu.context_type_id=188 
-- and wu.context_id IN (1413701, 1204130)
and finished_by_id is not null
group by wu.context_id) AS a
join sp_web_external.sp_web_spinny_auth_user sau on sau.id = a.finished_by_id
)iss on iss.context_id = bl.id

left join sp_web_external.sp_web_spinny_auth_contactdetails cd on cd.id = bl.contact_details_id
left join sp_web_external.sp_web_django_content_type dct on dct.id = bl.source_object_type_id
left join (select buy_lead_id, created_by_id from sp_web_external.sp_web_visits_visit v where visit_start_time is not null and visit_type_id=2 and scheduled_visit=1 group by buy_lead_id, created_by_id)v on v.buy_lead_id = cp.buy_lead_id
left join sp_web_external.sp_web_spinny_auth_user sau on sau.id = v.created_by_id
left join sp_web_external.sp_web_spinny_auth_user sau2 on sau2.id = cp.created_by_id
left join sp_web_external.sp_web_listing_procurredcar pc on pc.lead_id = cp.sell_lead_id
left join (select 
target_object_id sell_lead_id
,value listing_price
from sp_web_external.sp_web_pricing_pricevalue pv 
join 
(select max(pv.id) max_id from sp_web_external.sp_web_pricing_pricevalue pv 
where label_id = 1 and target_object_type_id=27
and created_by_id is not null
-- and created_on>='2020-01-01'
-- and target_object_id in (select sell_lead_id from buy_lead_carpurchase cp join buy_lead_buylead where category = 'bcm' and cp.created_time>='2020-10-01')
group by target_object_id
)mx on mx.max_id = pv.id)lpr on lpr.sell_lead_id = cp.sell_lead_id
left join
(select po.context_id deal_id
,SUM(pp.amount) token_amount,date(min(pp.payment_date)) token_d, date(min(pp.last_updated)) payment_last_updated

from sp_web_external.sp_web_payments_paymentorder po 
left join sp_web_external.sp_web_payments_payment pp on pp.order_id=po.id
left join sp_web_external.sp_web_spinny_auth_user sau on sau.id=po.created_by_id
left join sp_web_external.sp_web_spinny_auth_user sau2 on sau2.id=pp.created_by_id
left join sp_web_external.sp_web_payments_paymentitem pi on pi.id=pp.item_id
left join sp_web_external.sp_web_django_content_type dct on dct.id=po.context_type_id
where po.context_type_id=238
and name = 'Token' and pp.status = 'approved'
group by context_id
)pp on pp.deal_id=cp.id



left join sp_web_external.sp_web_payments_tradingpartner tp on tp.id = pc.trading_partner_id
left join sp_web_external.sp_web_sp_payments_contract ppc on ppc.trade_object_id = cp.id
LEFT JOIN (SELECT * FROM sp_web_external.sp_web_sp_payments_receivables WHERE status = 'approved') pr ON ppc.id=pr.contract_id
LEFt JOIN sp_web_external.sp_web_buy_lead_carpurchaserefundrequest AS cpr ON cpr.deal_id = cp.id
LEFT JOIN (SELECT lead_id, MAX(insurance_validity) AS insurance_validity_date FROM sp_web_external.sp_web_listing_registrationverification GROUP BY lead_id) AS lrv ON lrv.lead_id = cp.sell_lead_id


left join (select distinct
sla.id loan_id,com.deal_id
,sla.created_at + interval '5.5 hour' loan_confirmation_date
,NULLIF(spinny_login_date,bank_login_date) login_date
,NULLIF(spinny_rejected_date,rejected_date) rejected_date
,approved_date
,bb.display_name loan_vendor
,net_disbursed_amount
,CASE WHEN self_loan=1 THEN 'Yes-Self Loan'
    WHEN loan_facilitation=1 THEN 'Yes' ELSE 'No' END AS loanyn
,CASE
when sla.lead_status = 0 and labla.status = 0 and deal_status = 'Deal Cancelled' then 'Customer Not Interested'
when sla.lead_status = 0 and labla.status = 0 and deal_status = 'Token Collected' then 'Loan Logged In'
when sla.lead_status = 1 and labla.status = 0 and deal_status = 'Deal Cancelled' then 'Customer Not Interested'
when sla.lead_status = 1 and labla.status = 0 and deal_status = 'Delivery Done' then 'Customer Not Interested'
when sla.lead_status = 3 and labla.status = 0 and deal_status = 'Deal Cancelled' then 'Application Rejected - Loan not possible'
when sla.lead_status = 4 and labla.status = 0 and deal_status = 'Deal Cancelled' then 'Customer Not Interested'
when sla.lead_status = 4 and labla.status = 0 and deal_status = 'Token Collected' then 'Customer Not Interested'
when sla.lead_status = 5 and labla.status = 0 and deal_status = 'Deal Cancelled' then 'Customer Not Interested'
when sla.lead_status = 5 and labla.status = 0 and deal_status = 'Token Collected' then 'Loan Logged In'
when sla.lead_status = 6 and labla.status = 0 and deal_status = 'Deal Cancelled' then 'Customer Not Interested'
when sla.lead_status = 6 and labla.status = 0 and deal_status = 'Token Collected' then 'Loan Logged In'
when sla.lead_status = 6 and labla.status = 0 and deal_status = 'Delivery Done' then 'Loan Logged In'
when sla.lead_status = 1 and labla.status = 1 and deal_status = 'Deal Cancelled' then 'Customer Not Interested'
when sla.lead_status = 1 and labla.status = 1 and deal_status = 'Delivery Done' then 'Customer Not Interested'
when sla.lead_status = 3 and labla.status = 1 and deal_status = 'Deal Cancelled' then 'Application Rejected - Loan not possible'
when sla.lead_status = 4 and labla.status = 1 and deal_status = 'Deal Cancelled' then 'Customer Not Interested'
when sla.lead_status = 4 and labla.status = 1 and deal_status = 'Delivery Done' then 'Customer Not Interested'
when sla.lead_status = 5 and labla.status = 1 and deal_status = 'Token Collected' then 'Loan Logged In'
when sla.lead_status = 6 and labla.status = 1 and deal_status = 'Deal Cancelled' then 'Customer Not Interested'
when sla.lead_status = 6 and labla.status = 1 and deal_status = 'Token Collected' then 'Loan Logged In'
when sla.lead_status = 1 and labla.status = 2 and deal_status = 'Delivery Done' then 'Customer Not Interested'
when sla.lead_status = 1 and labla.status = 2 and deal_status = 'Deal Cancelled' then 'Customer Not Interested'
when sla.lead_status = 4 and labla.status = 2 and deal_status = 'Deal Cancelled' then 'Customer Not Interested'
when sla.lead_status = 6 and labla.status = 2 and deal_status = 'Token Collected' then 'Additional Documents Required'
when sla.lead_status = 1 and labla.status = 3 and deal_status = 'Deal Cancelled' then 'Customer Not Interested'
when sla.lead_status = 4 and labla.status = 3 and deal_status = 'Deal Cancelled' then 'Customer Not Interested'
when sla.lead_status = 6 and labla.status = 3 and deal_status = 'Deal Cancelled' then 'Customer Not Interested'
when sla.lead_status = 0 and labla.status = 4 and deal_status = 'Deal Cancelled' then 'Loan approved - Cancelled by Customer'
when sla.lead_status = 1 and labla.status = 4 and deal_status = 'Deal Cancelled' then 'Loan approved - Cancelled by Customer'
when sla.lead_status = 2 and labla.status = 4 and deal_status = 'Deal Cancelled' then 'Loan approved - Cancelled by Customer'
when sla.lead_status = 4 and labla.status = 4 and deal_status = 'Deal Cancelled' then 'Loan approved - Cancelled by Customer'
when sla.lead_status = 4 and labla.status = 4 and deal_status = 'Delivery Done' then 'Loan Approved'
when sla.lead_status = 5 and labla.status = 4 and deal_status = 'Deal Cancelled' then 'Loan approved - Cancelled by Customer'
when sla.lead_status = 6 and labla.status = 4 and deal_status = 'Token Collected' then 'Loan Approved'
when sla.lead_status = 6 and labla.status = 4 and deal_status = 'Delivery Done' then 'Loan Approved'
when sla.lead_status = 6 and labla.status = 4 and deal_status = 'Deal Cancelled' then 'Loan approved - Cancelled by Customer'
when sla.lead_status = 1 and labla.status = 5 and deal_status = 'Deal Cancelled' then 'Loan Rejected'
when sla.lead_status = 3 and labla.status = 5 and deal_status = 'Token Collected' then 'Loan Rejected'
when sla.lead_status = 3 and labla.status = 5 and deal_status = 'Deal Cancelled' then 'Loan Rejected'
when sla.lead_status = 4 and labla.status = 5 and deal_status = 'Deal Cancelled' then 'Loan Rejected'
when sla.lead_status = 4 and labla.status = 5 and deal_status = 'Delivery Done' then 'Loan Rejected'
when sla.lead_status = 4 and labla.status = 5 and deal_status = 'Token Collected' then 'Loan Rejected'
when sla.lead_status = 6 and labla.status = 5 and deal_status = 'Token Collected' then 'Loan Rejected'
when sla.lead_status = 6 and labla.status = 5 and deal_status = 'Delivery Done' then 'Loan Rejected'
when sla.lead_status = 6 and labla.status = 5 and deal_status = 'Deal Cancelled' then 'Loan Rejected'
when sla.lead_status = 0 and labla.status = 6 and deal_status = 'Delivery Done' then 'Loan Kit Signed'
when sla.lead_status = 4 and labla.status = 6 and deal_status = 'Deal Cancelled' then 'Loan approved - Cancelled by Customer'
when sla.lead_status = 4 and labla.status = 6 and deal_status = 'Delivery Done' then 'Loan Kit Signed'
when sla.lead_status = 4 and labla.status = 6 and deal_status = 'Token Collected' then 'Loan Kit Signed'
when sla.lead_status = 6 and labla.status = 6 and deal_status = 'Deal Cancelled' then 'Loan approved - Cancelled by Customer'
when sla.lead_status = 6 and labla.status = 6 and deal_status = 'Delivery Done' then 'Loan Kit Signed'
when sla.lead_status = 6 and labla.status = 6 and deal_status = 'Token Collected' then 'Loan Kit Signed'
when sla.lead_status = 6 and labla.status = 6 and deal_status = 'Car Returned' then 'Loan Kit Signed'
when sla.lead_status = 0 and labla.status = 7 and deal_status = 'Delivery Done' then 'Loan Disbursed'
when sla.lead_status = 5 and labla.status = 7 and deal_status = 'Delivery Done' then 'Loan Disbursed'
when sla.lead_status = 6 and labla.status = 7 and deal_status = 'Deal Cancelled' then 'Loan approved - Cancelled by Customer'
when sla.lead_status = 6 and labla.status = 7 and deal_status = 'Delivery Done' then 'Loan Disbursed'
when sla.lead_status = 6 and labla.status = 7 and deal_status = 'Token Collected' then 'Loan Disbursed'
when sla.lead_status = 6 and labla.status = 7 and deal_status = 'Car Returned' then 'Loan Disbursed'
when sla.lead_status = 1 and labla.status = 8 and deal_status = 'Deal Cancelled' then 'Loan approved - Cancelled by Customer'
when sla.lead_status = 1 and labla.status = 8 and deal_status = 'Delivery Done' then 'Loan approved - Cancelled by Customer'
when sla.lead_status = 4 and labla.status = 8 and deal_status = 'Deal Cancelled' then 'Loan approved - Cancelled by Customer'
when sla.lead_status = 4 and labla.status = 8 and deal_status = 'Delivery Done' then 'Loan approved - Cancelled by Customer'
when sla.lead_status = 0 and deal_status = 'default-status' then 'Application received'
when sla.lead_status = 0 and deal_status = 'Token Collected' then 'Application received'
when sla.lead_status = 0 and deal_status = 'Delivery Done' then 'Application received'
when sla.lead_status = 0 and deal_status = 'Car Returned' then 'Application received'
when sla.lead_status = 0 and deal_status = 'Car Delivered' then 'Application received'
when sla.lead_status = 0 and deal_status = 'Deal Cancelled' then 'Customer Not Interested'
when sla.lead_status = 1 and deal_status = 'Token Collected' then 'Customer Not Interested'
when sla.lead_status = 1 and deal_status = 'Delivery Done' then 'Customer Not Interested'
when sla.lead_status = 1 and deal_status = 'Deal Cancelled' then 'Customer Not Interested'
when sla.lead_status = 2 and deal_status = 'Token Collected' then 'Application received'
when sla.lead_status = 2 and deal_status = 'Deal Cancelled' then 'Customer Not Interested'
when sla.lead_status = 3 and deal_status = 'Deal Cancelled' then 'Application Rejected - Loan not possible'
when sla.lead_status = 4 and deal_status = 'Token Collected' then 'AUTO_CLOSED'
when sla.lead_status = 4 and deal_status = 'Deal Cancelled' then 'AUTO_CLOSED'
when sla.lead_status = 4 and deal_status = 'Delivery Done' then 'AUTO_CLOSED'
when sla.lead_status = 5 and deal_status = 'Deal Cancelled' then 'Customer Not Interested'
when sla.lead_status = 5 and deal_status = 'Token Collected' then 'Documents received'
when sla.lead_status = 5 and deal_status = 'Delivery Done' then 'Documents received'
when sla.lead_status = 6 and deal_status = 'Token Collected' then 'Loan Logged In'
when sla.lead_status = 6 and deal_status = 'Delivery Done' then 'Loan Logged In'
ELSE 'Others' END AS loan_flow_status
from (select * from sp_web_external.sp_web_loan_application_spinnyloanapplication sl
where sl.id in (select max(id) from sp_web_external.sp_web_loan_application_spinnyloanapplication group by deal_id)) sla
LEFT JOIN sp_web_external.sp_web_spinny_auth_user lo ON lo.id = sla.lead_owner_id
left join (select * from sp_web_external.sp_web_loan_application_bankloanapplication la where la.id in 
(select max(id) from sp_web_external.sp_web_loan_application_bankloanapplication group by spinny_loan_application_id))
labla on labla.spinny_loan_application_id = sla.id
left join sp_web_external.sp_web_bank_bank bb on bb.id = labla.bank_id
left join (select max(created_at) + interval '5.5 hour' approved_date,bank_loan_application_id
    from sp_web_external.sp_web_loan_application_spinnyloanapplicationlog where bank_loan_status = 4 group by bank_loan_application_id) apda on apda.bank_loan_application_id = labla.id 
left join (select min(created_at) + interval '5.5 hour' rejected_date,bank_loan_application_id
    from sp_web_external.sp_web_loan_application_spinnyloanapplicationlog where bank_loan_status = 5 group by bank_loan_application_id) reda on reda.bank_loan_application_id = labla.id 
left join (select max(created_at) + interval '5.5 hour' bank_login_date,bank_loan_application_id
    from sp_web_external.sp_web_loan_application_spinnyloanapplicationlog where bank_loan_status = 0 group by bank_loan_application_id) blda on blda.bank_loan_application_id = labla.id 
left join (select min(created_at) + interval '5.5 hour' spinny_rejected_date,loan_application_id
    from sp_web_external.sp_web_loan_application_spinnyloanapplicationlog where spinny_loan_status = 3 group by loan_application_id) sreda on sreda.loan_application_id = sla.id 
left join (select max(created_at) + interval '5.5 hour' spinny_login_date,loan_application_id
    from sp_web_external.sp_web_loan_application_spinnyloanapplicationlog where spinny_loan_status = 6 group by loan_application_id) slda on slda.loan_application_id = sla.id 
left join (select min(created_at) + interval '5.5 hour' docs_received_date,loan_application_id
    from sp_web_external.sp_web_loan_application_spinnyloanapplicationlog where spinny_loan_status = 5 group by loan_application_id) drda on drda.loan_application_id = sla.id 
    
join (
select cp.id deal_id,st.description deal_status
,loan_facilitation
, cp.self_loan
from sp_web_external.sp_web_buy_lead_carpurchase cp
join sp_web_external.sp_web_status_status st on st.id = cp.status_id
)com on 
com.deal_id = sla.deal_id 

order by sla.id desc,labla.id desc)loan on loan.deal_id = cp.id

left join (select bl.id
,case 
when dct.id = 246 then 
case
    when ww.url like '%truebil%' then 'Truebil'
    when ww.url like '%olx%' then 'Olx'
    when ww.url like '%cardekho%' then 'CarDekho'
    when ww.url like '%cartrade%' then 'CarTrade'
    when ww.url like '%Direct%' then 'Direct'
    else ww.url 
end
when dct.id = 319 then elelp.display_name
when dct.id = 321 then elelp2.display_name
when dct.id = 356 then 'Inbound'
WHEN dct.id = 301 then 'Walk-in'
when dct.id = 342 then 'Whatsapp'
when dct.id = 5 then 'Assured Redirect'
when dct.id = 187 then 'Spinny Filter'
when dct.id = 60 then 'Buyrequest'
-- 60	listing	buyrequest
-- 187	spinny_filters	filter
-- 136	callback	callback
-- 197	car_finance	carfinance
-- 27	listing	lead
-- 249	reference	reference
-- 309	shortlist shortlist
when dct.id in (249,27,197,136,309) then 'Other Spinny Direct'
else 'Others'
end as sub_sub_source_f
,case 
when dct.id = 246 then 
concat(
case
    when ww.url like '%truebil%' then 'Truebil'
    when ww.url like '%olx%' then 'Olx'
    when ww.url like '%cardekho%' then 'CarDekho'
    when ww.url like '%cartrade%' then 'CarTrade'
    when ww.url like '%Direct%' then 'Direct'
    else ww.url end
    ,' - Webarticle') 
when dct.id = 319 then concat(elelp.display_name,' - External Platform')
when dct.id = 321 then concat(elelp2.display_name,' - External Platform')
when dct.id = 356 then 'Inbound'
WHEN dct.id = 301 then 'Walk-in'
when dct.id = 342 then 'Whatsapp'
when dct.id = 5 then 'Assured Redirect'
when dct.id = 187 then 'Spinny Filter'
when dct.id = 60 then 'Buyrequest'
-- 60	listing	buyrequest
-- 187	spinny_filters	filter
-- 136	callback	callback
-- 197	car_finance	carfinance
-- 27	listing	lead
-- 249	reference	reference
-- 309	shortlist shortlist
when dct.id in (249,27,197,136,309) then 'Other Spinny Direct'
else 'Others'
end as sub_sub_source

from sp_web_external.sp_web_buy_lead_buylead bl 
join sp_web_external.sp_web_spinny_auth_contactdetails cd on cd.id = bl.contact_details_id
join sp_web_external.sp_web_django_content_type dct on dct.id = source_object_type_id
left join sp_web_external.sp_web_webresults_webarticle ww on ww.id = source_object_id and source_object_type_id = 246
left join sp_web_external.sp_web_external_listing_externallistingplatform elelp on elelp.id = source_object_id and source_object_type_id = 319
left join sp_web_external.sp_web_external_listing_externalbuyrequest elebr on elebr.id = source_object_id and source_object_type_id = 321
left join sp_web_external.sp_web_external_listing_externallisting elel on elel.id = elebr.listing_id
left join sp_web_external.sp_web_external_listing_listingplatformaccounts ellpa on ellpa.id = elel.account_id
left join sp_web_external.sp_web_external_listing_externallistingplatform elelp2 on elelp2.id = ellpa.platform_id
)s on s.id = bl.id
/*
left join (select a.object_pk deal_id, array_agg(concat(EXTRACT(DAY FROM submit_date)," - ",comment) , ' | ') comment from sp_web_external.sp_web_django_comments a
where content_type_id = 238
and DATE(submit_date)>='2020-01-01'
group by object_pk
)com on com.deal_id = cp.id
*/
where DATE(cp.created_time)>='2021-10-01' AND bl.category = 'bcm'
AND st.description NOT IN ('deal requested', 'deal expired')

group by bl.id, cp.id,
st.description
,cp.sell_lead_id
,registration_no
,ah.display_name 
,mm.display_name 
,mmo.display_name
,mv.display_name
,lp.color
,make_year
,mileage
,lp.fuel_type
,no_of_owners
,cp.buy_lead_id
,s.sub_sub_source_f
,sub_sub_source
,cd.name
,cd.contact_number
,inside_sales
,sau2.full_name
,pc.price
,listing_price
,cp.final_amount
,insurance_renewal
,insurance_renewal_amount
,cp.created_time 
,pp.token_amount
-- ,pp.token_date
,target_delivery_time
,loan_interest
,Self_loan_interest
,loan_vendor
,loan_flow_status
,login_date
, approved_rejected_date
,net_disbursed_amount
,cp.delivery_time
,deal_cancellation_time 
,cp.cancellation_reason
,car_return_time
,car_return_reason

, car_live_date
,loan_confirmation_date

,bl.category
,l.procurement_category
,cpr.status
,lrv.insurance_validity_date

-- ,pr.amount
-- ,cp.payment_status
    
    '''
    )
print("query ended 1")
deal_df = pd.DataFrame(cursor, columns=['deal_id', 'deal_status', 'sell_lead_id', 'registration_no', 'deal_hub', 'make', 'model'
                                        ,'variant', 'color', 'make_year', 'mileage', 'fuel_type', 'no_of_owners', 'buy_lead_id', 'sub_sub_source_f', 'buyer_name'
                                        ,'buyer_number', 'inside_sales', 'hub_sales', 'procurement_price', 'listing_price'
                                        ,'sell_price', 'paper_transfer_cost', 'warranty_cost', 'insurance_validity_date'
                                        ,'payment_amount', 'token_amount', 'car_live_date', 'token_date','edd1'
                                        ,'loan_interest', 'loan_vendor', 'loan_flow_status', 'login_date','approved_rejected_date'
                                        ,'net_disbursed_amount', 'self_loan_interest', 'refund_status', 'delivery_time'
                                        ,'deal_cancellation_time', 'cancellation_reason', 'car_return_time'
                                        ,'car_return_reason'])



#%%

df = pd.merge(deal_df, refurb_df, left_on='sell_lead_id', right_on='sell_lead_id', how='left')
#df = df.drop(['sell_lead_id'], axis=1)
df = df.drop(['reg_no'], axis=1)
df['car_live_date'] = pd.to_datetime(df['car_live_date'])


df = df.replace(np.nan,"")
#%%
gc = pygsheets.authorize(service_file=r"/home/abhishekkumar2/bcm/bcm_pricing/keys.json")

#%%
#Rishabh Sheets
sh_ms = gc.open('DCT V_2.0 Raw 4.4')
ms=sh_ms.worksheet_by_title('MS')
ms.clear(start ='A',end='AR')
ms.set_dataframe(df,start=(1,1))      

sh_ms = gc.open('Demand Control Tower_Truebil_v2.0')
ms=sh_ms.worksheet_by_title('MS')
ms.clear(start ='A',end='AR')
ms.set_dataframe(df,start=(1,1)) 

sh_ms = gc.open('DCT V_2.0 Raw 4.5')
ms=sh_ms.worksheet_by_title('MS')
ms.clear(start ='A',end='AR')
ms.set_dataframe(df,start=(1,1)) 


day_180 = datetime.datetime.now() - datetime.timedelta(90)
sh_ms = gc.open('Margin Comparison ')
ms=sh_ms.worksheet_by_title('MS')
ms.clear(start ='A',end='AR')
df_del = df[df['deal_status'] == 'Delivered']
df_del['delivery_time'] = pd.to_datetime(df_del['delivery_time'])
df_del = df_del[df_del['delivery_time']>=day_180]
ms.set_dataframe(df_del,start=(1,1))

sh_ms = gc.open('DCT V_2.0 Raw 2.2')
ms=sh_ms.worksheet_by_title('MS')
ms.clear(start ='A',end='AR')
ms.set_dataframe(df,start=(1,1))

#sh_ms = gc.open('FP trend - Investor data')
#ms=sh_ms.worksheet_by_title('MS')
#ms.clear(start ='A',end='AR')
#ms.set_dataframe(df,start=(1,1))



#%%
#Adrija Sheets

sh_ms = gc.open('DCT Raw 1.0')
ms=sh_ms.worksheet_by_title('MS Dump')
ms.clear(start ='A',end='AR')
ms.set_dataframe(df,start=(1,1))       

sh_ms = gc.open('DCT Raw 2.0')
ms=sh_ms.worksheet_by_title('MS Dump')
ms.clear(start ='A',end='AR')
ms.set_dataframe(df,start=(1,1))             


sh_ms = gc.open('DCT Raw 3.0')
ms=sh_ms.worksheet_by_title('MS Dump')
ms.clear(start ='A',end='AR')
ms.set_dataframe(df,start=(1,1))            


sh_ms = gc.open('DCT Raw 4.0')
ms=sh_ms.worksheet_by_title('MS Dump')
ms.clear(start ='A',end='AR')
ms.set_dataframe(df,start=(1,1))             



sh_ms = gc.open('Raw DCT Margin 4.0')
ms=sh_ms.worksheet_by_title('MS Dump')
ms.clear(start ='A',end='AR')
ms.set_dataframe(df,start=(1,1))            



sh_ms = gc.open(' Raw DCT Margin 4.1')
ms=sh_ms.worksheet_by_title('MS Dump')
ms.clear(start ='A',end='AR')
ms.set_dataframe(df,start=(1,1))            



sh_ms = gc.open('Raw DCT Margin 4.2 Final File ')
ms=sh_ms.worksheet_by_title('MS Dump')
ms.clear(start ='A',end='AR')
ms.set_dataframe(df,start=(1,1))            



sh_ms = gc.open('Raw DCT Margin 4.3')
ms=sh_ms.worksheet_by_title('MS Dump')
ms.clear(start ='A',end='AR')
ms.set_dataframe(df,start=(1,1))   


price_rf = gc.open('Pricing - APP_ARF')
pr=price_rf.worksheet_by_title('Actual RF cost')
#ms.clear(start ='A',end='AR')
pr.set_dataframe(refurb_df,start=(1,1)) 



end = datetime.datetime.now()
print('execution time: ', end-start)
print("last run time:", end)
print('DCT_V2.0_MS')

end = datetime.datetime.now() + pd.Timedelta(hours=5, minutes=30)
with open('/home/abhishekkumar2/bcm/bcm_pricing/output_code.txt', 'a') as f:
    f.write('{} - code ended \n'.format(end))