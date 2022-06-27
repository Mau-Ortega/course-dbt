# WEEK 1 PROJECT

### How many users do we have?

``` sql
/* The querry shown below helps us quantify the numbers of distinct users found in our greenery__users 
staging model. 
The number of distinct users is 130 
*/
select
count(distinct user_id)
from dbt.dbt_mauricio_o.stg_greenery__users;

```
Answer: 130

### On average, how many orders do we receive per hour?

``` sql
/*The following querry helps us quantify the avg number of orders we recieve per hour, to do this
we will use the date_trunc function which allows us to round our timestamps to our specified precision,
in this case: 'hour'.
The average number of orders is: 7.52
*/

with orders_per_hour as
(
select

date_trunc('hour',created_at) as created_at_hour,
count(distinct order_id) as num_orders
from dbt.dbt_mauricio_o.stg_greenery__orders
group by 1
)

select
avg(num_orders) as avg_order_num

from orders_per_hour;
```
Answer: 7.52

### On average, how long does an order take from being placed to being delivered?

``` sql
/* The following querry quantifies the average delivery time for our greenery__orders view. 
The average time is: 3 Days 21h:24m:11s
*/
select

avg(delivered_at - created_at)

from dbt.dbt_mauricio_o.stg_greenery__orders
where status = 'delivered'
;

```
Answer: 3 Days 21h: 24m: 11s

### How many users have only made one purchase? Two purchases? Three+ purchases?

``` sql
/* The following querry helps us calculate the number of users that have placed n-number of purchases.
The result is the following: 
1: 25 users
2: 28 users
3: 34 users
4: 20 users
5: 10 users
6: 2 users
7: 4 users
8: 1 user
*/

-- count number of orders per user by grouping distinct order_id's per user_id
with orders_per_user as
(
select

user_id,
count(distinct order_id) as num_orders

from dbt.dbt_mauricio_o.stg_greenery__orders

group by 1
)

-- group number of orders by distinct cound of users.
select
num_orders,
count(distinct user_id) as num_users

from orders_per_user
group by 1
order by 1 asc
;
```
Answer: 1: 25 users; 2: 28 users; 3: 34 users; 4: 20 users; 5: 10 users; 6: 2 users; 7: 4 users; 8: 1 user


### On average, how many unique sessions do we have per hour?

``` sql
/*For our final question, we have to calculate the average number of sessions per hour. To do this
we first have to understand how a session works. A sessions is made up of multiple events, therefore,
we should base ourselves on the first event per session. We do this by numbering each session from
its first to its last event.
The average number of sessions is: 11.79
*/

-- session_id partitioned and ranked
with first_event as
(
select
*,
row_number() over (partition by session_id order by created_at asc) as n
from dbt.dbt_mauricio_o.stg_greenery__events
),

-- Timestamp is rounded to its hour part and n = 1 -> first event per session.
-- Number of sessions are grouped by hour
num_sessions as

(
select
date_trunc('hour',created_at) as created_at_hour,
count(distinct session_id) as num_sessions

from first_event
where n= 1
group by 1

)

-- Average number of sessions calculated
select
avg(num_sessions)

from num_sessions
;
```
Answer: 11.79

# WEEK 2 PROJECT

### What is our user repeat rate??
```sql
with num_orders as
(
select

user_id,
count(distinct order_id) as num_orders

from dbt.dbt_mauricio_o.int_users_orders
where status is not null
group by 1
)
select

count(distinct case when num_orders >= 2 then user_id end)::float/count(distinct user_id)::float as repeat_rate

from num_orders
```
Answer: .80 or 80%

### What are good indicators of a user who will likely purchase again? What about indicators of users who are likely NOT to purchase again? If you had more data, what features would you want to look into to answer this question?
We can check a users acumulated purchases in time and compare it vs the average active users acumulated purchases to see when we can expect a user to recur again.


# WEEK 3 PROJECT

### What is our user repeat rate??
```sql
select

count(distinct case when n_checkout = 1 then session_id end)::float/count(distinct session_id)::float

from dbt.dbt_mauricio_o.int_session_events_basic_agg;
```
Answer: .62

### What is our conversion rate by product??
For this question, a facts_product_converion table was used that simplified the number of times a product was viewed or added to cart. With this logic already create, all that was really needed was to group the information by product and sum the number of views and checkouts to calculate the conversion rate:
```sql
select

product_id,
name,
sum(nb_added_to_cart)/sum(nb_times_viewed) as conversion_rate

from dbt.dbt_mauricio_o.facts_product_conversion
group by 1,2
order by 3 desc
```
Answer:
String of pearls
0.40366972477064220183
Arrow Head
0.37864077669902912621
Bamboo
0.37837837837837837838
Calathea Makoyana
0.37647058823529411765
Cactus
0.36781609195402298851
Rubber Plant
0.36363636363636363636
Aloe Vera
0.35643564356435643564
Majesty Palm
0.35514018691588785047
Bird of Paradise
0.35483870967741935484
Dragon Tree
0.35416666666666666667
Boston Fern
0.35051546391752577320
ZZ Plant
0.35000000000000000000
Pilea Peperomioides
0.34782608695652173913
Devil's Ivy
0.34782608695652173913
Monstera
0.34666666666666666667
Peace Lily
0.34313725490196078431
Jade Plant
0.34285714285714285714
Angel Wings Begonia
0.34042553191489361702
Ficus
0.33980582524271844660
Fiddle Leaf Fig
0.33707865168539325843
Spider Plant
0.33707865168539325843
Philodendron
0.33684210526315789474
Birds Nest Fern
0.33333333333333333333
Alocasia Polly
0.33333333333333333333
Pink Anthurium
0.33333333333333333333
Orchid
0.33035714285714285714
Snake Plant
0.31775700934579439252
Money Tree
0.31707317073170731707
Ponytail Palm
0.29702970297029702970
Pothos
0.27272727272727272727