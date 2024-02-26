use data_bank;

-- 1. How many unique nodes are there on the Data Bank system?
select * from customer_nodes;

-- gives total sum of distint regions aggregated for all regions
with nodes_region as (select count(distinct node_id) as nodes_per_region
					from customer_nodes
					group by region_id)
select sum(nodes_per_region) as unique_nodes
from nodes_region;

-- 2. What is the number of nodes per region?

-- this gives per node count per regions  
select r.region_name, count(distinct n.node_id) nodes_per_region
from customer_nodes n, regions r
where n.region_id = r.region_id
group by r.region_name;

-- 3. How many customers are allocated to each region?
select * from customer_nodes;
select r.region_name, count(distinct n.customer_id) as customer_per_region 
from customer_nodes n, regions r
where n.region_id = r.region_id
group by r.region_name
order by customer_per_region desc;

-- 4. How many days on average are customers reallocated to a different node?
-- avg days per node is 14.63 when 500 row with end_year as 9999 is ignored
select round(avg(datediff(end_date, start_date)), 2) as avg_days_nodes
from customer_nodes
where year(end_date) <> 9999;

-- returns rows where end_date year is not 9999 
select * from customer_nodes
where year(end_date) <> 9999;

-- returns average year of end_date year is not 9999; answer is 2020
select avg(year(end_date))
from customer_nodes
where year(end_date) <> 9999;


-- avg days per node goes up to 49.26 days when 9999 year in end date is changed to 2020
with edited_date as (select *,
case when year(end_date) = 9999 then date_format(end_date,'2020-%m-%d')
when year(end_date) = 2020 then end_date 
end as end_year_2020
from customer_nodes)

select round(avg(datediff(end_year_2020, start_date)), 2) as avg_days_nodes
from edited_date;

-- 5. What is the median, 80th and 95th percentile for this same reallocation days metric for each region?
select * from customer_nodes;

select *, datediff(end_date, start_date) as days_reallocation 
from customer_nodes
where year(end_date) <> 9999;

with reallocate as (select *, datediff(end_date, start_date) as days_reallocation 
from customer_nodes
where year(end_date) <> 9999) 

select r.region_id, r.days_reallocation, 
percent_rank() over(partition by r.region_id order by r.days_reallocation) ptile
from reallocate r; 

-- Median per region
with reallocate as (select *, datediff(end_date, start_date) days_per_node,
ntile(2) over(partition by region_id order by datediff(end_date, start_date)) ntile_
from customer_nodes
where year(end_date) <> 9999)

select regions.region_name, min(reallocate.days_per_node) 
from reallocate, regions 
where reallocate.region_id = regions.region_id
and ntile_ = 2
group by regions.region_name;


-- 80th percentile
with reallocate as (select *, datediff(end_date, start_date) days_per_node,
ntile(5) over(partition by region_id order by datediff(end_date, start_date)) ntile_
from customer_nodes
where year(end_date) <> 9999)

select regions.region_name, min(reallocate.days_per_node) 
from reallocate, regions 
where reallocate.region_id = regions.region_id
and ntile_ = 5
group by regions.region_name;
 
-- 95th percentile
with reallocate as (select *, datediff(end_date, start_date) days_per_node,
ntile(20) over(partition by region_id order by datediff(end_date, start_date)) ntile_
from customer_nodes
where year(end_date) <> 9999)

select regions.region_name, min(reallocate.days_per_node) 
from reallocate, regions 
where reallocate.region_id = regions.region_id
and ntile_ = 20
group by regions.region_name;

-- B. Customer Transactions
-- 1. What is the unique count and total amount for each transaction type?
select * from customer_transactions;

select txn_type, count(distinct txn_type) as transaction_type, count(txn_type) as Number_of_each_transaction
from customer_transactions
group by txn_type;

-- 2. What is the average total historical deposit counts and amounts for all customers?

select avg(txn_amount)
from customer_transactions
where txn_type='deposit';

with depo_number as (select customer_id, count(txn_type) as no_of_deposit, sum(txn_amount) as total_deposit
from customer_transactions
where txn_type = 'deposit'
group by customer_id
order by customer_id)

select round(avg(no_of_deposit), 2) as avg_number_of_deposits_by_customer, round(avg(total_deposit), 2) as avg_deposit
from depo_number;

-- rechecking my avg count and sum calculation for the deposits
-- the code below gives wrong value of avg deposit of 508.86 as it is dividing sum of deposits by total number of deposit (2671) 
-- and not by distinct number of customers who made the deposits
select avg(txn_amount) 
from customer_transactions
where txn_type = 'deposit';

-- distinct number of customers who made the deposits are 500
select count(distinct customer_id)
from customer_transactions
where txn_type = 'deposit';

-- this gives total number of times deposit is made (2671) or total number of times deposits were made by all the customers 
select count(customer_id)
from customer_transactions
where txn_type = 'deposit';

-- total sum of deposits is 1359168
select sum(txn_amount)
from customer_transactions
where txn_type = 'deposit';

-- Avg deposit is 1359168/500 and not 1359168/2671

-- 3. For each month - how many Data Bank customers make more than 1 deposit and either 1 purchase or 1 withdrawal in a single month?
-- Distinct number of customers in customer_transaction table
select count(distinct customer_id) as Number_of_customers from customer_transactions; 

-- Calculating number of deposits per month
with per_month_depo as (select customer_id, txn_type, monthname(txn_date) as month_, 
						count(txn_type)
						from customer_transactions
						where txn_type = 'deposit'
						group by monthname(txn_date), customer_id
						having count(txn_type) > 1)

select month_, count(customer_id) 
from per_month_depo
group by month_;

-- This is the answer to Q3
with depo as (select customer_id, monthname(txn_date) month_, count(txn_type) as per_month_depo
from customer_transactions
where txn_type = 'deposit'
group by customer_id, monthname(txn_date)
having count(txn_type) > 1),

wid_pur as (select customer_id, monthname(txn_date) month_, count(txn_type) as per_month_pur
from customer_transactions
where txn_type <> 'deposit'
group by customer_id, monthname(txn_date)
having count(txn_type) = 1)

select depo.month_, count(depo.customer_id)
from depo, wid_pur
where depo.customer_id = wid_pur.customer_id
and depo.month_ = wid_pur.month_
group by depo.month_;



-- 4. What is the closing balance for each customer at the end of the month?
select DISTINCT monthname(txn_date) from customer_transactions;

with trans as (select customer_id, txn_date, monthname(txn_date) as month_, txn_type,
CASE when txn_type = 'purchase' or txn_type = 'withdrawal' then txn_amount * -1
ELSE txn_amount
END as trans_amt
from customer_transactions
),
balances as (select *,
				sum(trans_amt) over(partition by customer_id order by txn_date) as monthly_bal
			from trans),
monthly_balance as (select *,
                last_value(monthly_bal) over(partition by customer_id, monthname(txn_date)) as final_bal
			from balances)

select customer_id, month_, final_bal
from monthly_balance
group by customer_id, month_, final_bal
order by customer_id, month_;

-- 5. What is the percentage of customers who increase their closing balance by more than 5%?

with trans as (select customer_id, txn_date, monthname(txn_date) as month_, txn_type,
CASE when txn_type = 'purchase' or txn_type = 'withdrawal' then txn_amount * -1
ELSE txn_amount
END as trans_amt
from customer_transactions
),
balances as (select *,
				sum(trans_amt) over(partition by customer_id order by txn_date) as monthly_bal
			from trans),
monthly_balance as (select *,
				first_value(monthly_bal) over(partition by customer_id, monthname(txn_date)) as first_bal,
                last_value(monthly_bal) over(partition by customer_id, monthname(txn_date)) as final_bal
			from balances),
percent_balance as (select *, ((final_bal - first_bal)/abs(first_bal)) as percent_bal from monthly_balance)

-- calculating percent of customers who increase closing balance by 5% on monthly balance
select round(((select count(customer_id) from percent_balance where percent_bal > 0.05) / count(customer_id)) * 100, 2) as bal_inc_5percent 
from percent_balance;

-- C. Data Allocation Challenge

-- ● Option 1: data is allocated based off the amount of money at the end of the previous month
-- how much data would have been required for each option on a monthly basis?

with trans as (select customer_id, txn_date, monthname(txn_date) as month_, txn_type,
CASE when txn_type = 'purchase' or txn_type = 'withdrawal' then txn_amount * -1
ELSE txn_amount
END as trans_amt
from customer_transactions
),
-- running customer balance column that includes the impact each transaction
real_time_bal as (select *,
					sum(trans_amt) over(partition by customer_id order by txn_date) as live_bal
                    -- sum(trans_amt) over(partition by customer_id order by month_) as month_bal
					from trans),
              

-- customer balance at the end of each month
lag_month_bal as (select *, 
						lag(live_bal) over(partition by customer_id order by month(txn_date)) as previous_month
						from real_time_bal),
                        
-- amount of money at the end of the previous month
previous_month_bal as (select *, 
					first_value(previous_month) over(partition by customer_id, month(txn_date)) as month_end
                    from lag_month_bal),

-- minimum, average and maximum values of the running balance for each customer
aggregate_bal as (select *,
					min(live_bal) over(partition by customer_id order by month(txn_date)) as min_bal,
                    avg(live_bal) over(partition by customer_id order by month(txn_date)) as avg_bal,
                    max(live_bal) over(partition by customer_id order by month(txn_date)) as max_bal					
                    from previous_month_bal),

-- lag of avg balance
lag_avg as (select *, 
			lag(avg_bal) over(partition by customer_id order by month(txn_date)) as previous_avg
			from aggregate_bal),

-- avg balance of previous 30 days has been presented in front of each month
previous_30day_avg as (select *, 
						first_value(previous_avg) over(partition by customer_id, month(txn_date)) as previous_month_avg
						from lag_avg)

-- Per month data required based on the amount of money at the end of the previous month
select month(txn_date), monthname(txn_date) as Month, round(sum(month_end), 2) option_1
from previous_30day_avg
group by month(txn_date), monthname(txn_date)
order by month(txn_date);

-- ● Option 2: data is allocated on the average amount of money kept in the account in the previous 30 days - 
-- how much data would have been required for each option on a monthly basis?

with trans as (select customer_id, txn_date, monthname(txn_date) as month_, txn_type,
CASE when txn_type = 'purchase' or txn_type = 'withdrawal' then txn_amount * -1
ELSE txn_amount
END as trans_amt
from customer_transactions
),
-- running customer balance column that includes the impact each transaction
real_time_bal as (select *,
					sum(trans_amt) over(partition by customer_id order by txn_date) as live_bal
                    -- sum(trans_amt) over(partition by customer_id order by month_) as month_bal
					from trans),
              

-- customer balance at the end of each month
lag_month_bal as (select *, 
						lag(live_bal) over(partition by customer_id order by month(txn_date)) as previous_month
						from real_time_bal),
                        
-- amount of money at the end of the previous month
previous_month_bal as (select *, 
					first_value(previous_month) over(partition by customer_id, month(txn_date)) as month_end
                    from lag_month_bal),

-- minimum, average and maximum values of the running balance for each customer
aggregate_bal as (select *,
					min(live_bal) over(partition by customer_id order by month(txn_date)) as min_bal,
                    avg(live_bal) over(partition by customer_id order by month(txn_date)) as avg_bal,
                    max(live_bal) over(partition by customer_id order by month(txn_date)) as max_bal					
                    from previous_month_bal),

-- lag of avg balance
lag_avg as (select *, 
			lag(avg_bal) over(partition by customer_id order by month(txn_date)) as previous_avg
			from aggregate_bal),

-- avg balance of previous 30 days has been presented in front of each month
previous_30day_avg as (select *, 
						first_value(previous_avg) over(partition by customer_id, month(txn_date)) as previous_month_avg
						from lag_avg)

-- Per month data required based on the average amount of money kept in the account in the previous 30 days                       
select month(txn_date), monthname(txn_date) as Month, round(sum(previous_month_avg), 2) option_2
from previous_30day_avg
group by month(txn_date), monthname(txn_date)
order by month(txn_date);

-- ● Option 3: data is updated real-time
-- how much data would have been required for each option on a monthly basis?

with trans as (select customer_id, txn_date, monthname(txn_date) as month_, txn_type,
CASE when txn_type = 'purchase' or txn_type = 'withdrawal' then txn_amount * -1
ELSE txn_amount
END as trans_amt
from customer_transactions
),
-- running customer balance column that includes the impact each transaction
real_time_bal as (select *,
					sum(trans_amt) over(partition by customer_id order by txn_date) as live_bal
                    -- sum(trans_amt) over(partition by customer_id order by month_) as month_bal
					from trans),
              

-- customer balance at the end of each month
lag_month_bal as (select *, 
						lag(live_bal) over(partition by customer_id order by month(txn_date)) as previous_month
						from real_time_bal),
                        
-- amount of money at the end of the previous month
previous_month_bal as (select *, 
					first_value(previous_month) over(partition by customer_id, month(txn_date)) as month_end
                    from lag_month_bal),

-- minimum, average and maximum values of the running balance for each customer
aggregate_bal as (select *,
					min(live_bal) over(partition by customer_id order by month(txn_date)) as min_bal,
                    avg(live_bal) over(partition by customer_id order by month(txn_date)) as avg_bal,
                    max(live_bal) over(partition by customer_id order by month(txn_date)) as max_bal					
                    from previous_month_bal),

-- lag of avg balance
lag_avg as (select *, 
			lag(avg_bal) over(partition by customer_id order by month(txn_date)) as previous_avg
			from aggregate_bal),

-- avg balance of previous 30 days has been presented in front of each month
previous_30day_avg as (select *, 
						first_value(previous_avg) over(partition by customer_id, month(txn_date)) as previous_month_avg
						from lag_avg)

-- Per month data required to based on updated real-time balance
select month(txn_date), monthname(txn_date) as Month, round(sum(live_bal), 2) option_3
from previous_30day_avg
group by month(txn_date), monthname(txn_date)
order by month(txn_date);


/*
D. Extra Challenge
If the annual interest rate is set at 6% and the Data Bank team wants to reward its customers by increasing their data allocation based off the interest
calculated on a daily basis at the end of each day, how much data would be required for this option on a monthly basis?
*/
with trans as (select customer_id, txn_date, monthname(txn_date) as month_, txn_type,
CASE when txn_type = 'purchase' or txn_type = 'withdrawal' then txn_amount * -1
ELSE txn_amount
END as trans_amt
from customer_transactions
),

real_time_bal as (select *,
					sum(trans_amt) over(partition by customer_id order by txn_date) as live_bal
					from trans),

days as (select *, datediff(txn_date, lag(txn_date) over(partition by customer_id order by txn_date)) as days_last_trans 
from real_time_bal),

lead_days as (select *, lead(days_last_trans) over(partition by customer_id order by txn_date) as days_next_trans 
				from days),

total as (select *, sum(live_bal * days_next_trans) over(partition by customer_id order by month(txn_date)) as total_bal,
					sum(days_next_trans) over(partition by customer_id, month(txn_date)) as days_per_period
					from lead_days),

 daily_bal as (select *, (total_bal div days_per_period) as avg_daily_bal
				from total),

interest_charge as (select *, round(((avg_daily_bal * 0.000164) * days_per_period), 2) as interest
					from daily_bal)

-- Data required based on interest calculated on daily basis
select month(txn_date), monthname(txn_date) as Month, round(sum(interest), 2) Interest_payment
from interest_charge
group by month(txn_date), monthname(txn_date)
order by month(txn_date);

























