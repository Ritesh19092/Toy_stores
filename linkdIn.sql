/* 
Step - 1 create a datebase 
*/

create database Toy_salesdb

/*
Step - 2 create tables for storing the data , keep datatype "Varchar(max)" for all field (it help to import inconsistent data)
*/

create table sales
(Sale_ID varchar(max),	Date varchar(max),	Store_ID varchar(max),	Product_ID varchar(max),Units varchar(max))

create table stores
(Store_ID varchar(max),	Store_Name varchar(max),Store_City varchar(max),Store_Location varchar(max),Store_Open_Date varchar(max))

create table products
(Product_ID varchar(max),	Product_Name varchar(max),	Product_Category varchar(max),Product_Cost varchar(max), Product_Price varchar(max))

create table inventory
(Store_id varchar(max), Product_id varchar(max), stock_on_hand varchar(max))

/*
Step - 3 insert the data into tables using bulk inserting method 
*/

bulk insert Sales
from 'C:\Users\Ritesh\Downloads\sales.csv'
with (fieldterminator=','  , Rowterminator='\n', firstrow=2, Maxerrors=40)

bulk insert products
from 'C:\Users\Ritesh\Downloads\products.csv'
with (fieldterminator=','  , Rowterminator='\n', firstrow=2, Maxerrors=40)

bulk insert Stores
from 'C:\Users\Ritesh\Downloads\stores.csv'
with (fieldterminator=','  , Rowterminator='\n', firstrow=2, Maxerrors=40)

bulk insert Inventory
from 'C:\Users\Ritesh\Downloads\inventory.csv'
with (fieldterminator=','  , Rowterminator='\n', firstrow=2, Maxerrors=40)

/*
step - 4 check the data weather it is correctly imported or not , if not than we have to import it correctly before starting the analysis 
*/

select * from sales 
select * from products
select * from stores
select * from inventory

select Column_name, Data_type
from Information_schema.columns
where table_name in ('Sales','Stores','Products','Inventory')

/*
Step - 5 change the datatype of the column according to the data present in the particular column.  Ex. int for numaric columns , date for date columns 

Step - 6 clean the data and correct incorrect values

*/

select * from Sales
where isnumeric(sale_id)=0

select top 10000 sale_id,
Replace(REPLACE( Sale_ID, SUBSTRING( Sale_ID, 
charindex('%[~,@,#,$,%,&,*,^,&,%,*,(,),-]%', Sale_ID), 1 ),''),'-',' ') from sales

UPDATE sales
SET Sale_ID = 
(
CASE 
WHEN Sale_ID LIKE '%[^0-9]%' 
THEN Replace(REPLACE( Sale_ID, SUBSTRING( Sale_ID, PATINDEX('%[~,@,#,$,%,&,*,^,&,%,*,(,),-]%', Sale_ID), 1 ),''),'-',' ')
ELSE [Sale_ID]
END
)

select * from sales
where Sale_id like '%[^0-9]%'

alter table sales alter column sale_id int not null

alter table sales
alter column product_id int not null

select date, convert(varchar(20), getdate(),23) from sales

select * from sales where isdate(date)=1

select date,convert(varchar, Try_convert(date, Date),23) from Sales

update sales set date=convert(varchar, Try_convert(date, Date),23) from Sales

alter table sales
alter column [date] date

select * from Sales

alter table sales
alter column store_id int

select top 10000 units,
 Replace(REPLACE( units, SUBSTRING( units, 
 Patindex('%[~,@,#,$,%,&,*,^,&,%,*,(,),-]%', units), 1 ),''),'-',' ') from sales

select * from sales
where isnumeric(units)=0

 update sales set units=Replace(REPLACE( units, SUBSTRING( units, 
 Patindex('%[~,@,#,$,%,&,*,^,&,%,*,(,),-,A-Z]%', units), 1 ),''),'-',' ')

alter table sales
alter column units int

 select * from sales
 where isnumeric(units)=0

select * from sales where date is null

select Column_name, Data_type
from Information_schema.columns
where table_name in ('Sales')

select * from Products

select * from Products
where isnumeric(product_id)=0

update products set product_id=CASE 
WHEN product_id LIKE '%[^0-9]%' 
THEN Replace(REPLACE( product_ID, SUBSTRING( product_ID, PATINDEX('%[~,@,#,$,%,&,*,^,&,%,*,(,),-]%', product_ID), 1 ),''),'-',' ')
ELSE [Product_id]
END

alter table products
alter column product_id int

select charindex('$',product_cost) from products
 
update products set product_price=substring(product_price,2,len(product_price)-1)

alter table products
alter column product_price decimal(7,2)

alter table products
alter column product_cost decimal(7,2)

select column_name, data_type
from information_schema.columns
where table_name='products'

select * from inventory

select Column_name , Data_type 
from information_schema.columns
where table_name='inventory'

alter table inventory alter column store_id int 

alter table inventory alter column stock_on_hand int

alter table inventory alter column product_id int
 
select * from stores

select Column_name , Data_type 
from information_schema.columns
where table_name='stores'

alter table stores alter column store_id int

select * from Stores where isdate(store_open_date)=0

select store_open_date, convert(varchar(20),store_open_date,23) from stores

alter table stores alter column store_open_date date



select * from sales

select  distinct store_location  from stores

select * from stores

select Column_name , Data_type 
from information_schema.columns
where table_name in ('sales','products')

with sales_per as (select stores.Store_Location , ( select sum(units) from sales where Store_ID = stores.Store_ID) as 'total_Un',
(select sum(units * product_price)
from sales
JOIN products on sales.Product_ID = products.Product_ID
where sales.Store_ID = stores.Store_ID) as 'total_rev'
from stores)

select store_location, sum(total_un) as 'sales_per_loc', sum(total_rev) as 'total_rev_loc' from sales_per
group by store_location
order by total_rev_loc desc

select * from sales

select * from products

select Store_City , Store_location,count(Store_Location) from stores
group by Store_City ,store_location

select Store_location,count(store_city) from stores
group by store_location

/*
Analysis NO. 1 

analyze the total units sold and total revenue generated for store location

Method :-

1. join tables sales , product and store using store_id 

2. sum of units according to the location as "total_unit_sold"

3. unit * product_price as "total_revenue"

4. sum of total_unit_sold & sum of total_revenue

Outcome : - 

store_location	sales_per_loc	total_rev_loc
Downtown	      628073	     9090649.03
Commercial	      240648		 3618206.82
Residential		  125402		 1831422.84
Airport			  96442			 1416997.08

*/

select Column_name , Data_type 
from information_schema.columns
where table_name in ('sales','products')

Select datepart(M,Date), datename(Month,Date) from sales

select products.product_id, product_name,product_category, datename(month,sales.date) as Sale_month, 
datepart(year,sales.date) as 'year', Sum(sales.units) as 'Total_sale'from sales 
join products 
on sales.product_id=products.product_id
group by products.product_id,products.product_name,products.product_category, Datename(month,sales.date),datepart(year,sales.date)

/*
Analysis NO. 2 

Check the monthly sales for each product and their category

Method :-

1. join the tables sales and product by using product_id 

2. Take month of sales from colunm date of sales table using datename method to show the month of sales

3. Take year of sales from colunm date of sales table using datepart method to show the year of sales

4. sum the unit column from table sales and arrange them according to the month and year using group by clause

Outcome : -

monopoly has has the lowest sales among the products 
*/

select * from sales

select * from products

select MAX(date) from sales

select DATEADD(MONTH , -6,'2023-09-30')

select sales.Store_id,sales.Date, stores.store_name,sum(sales.units) as 'Total_unit_sold', sum(sales.units * products.product_price) as 'Total_revenue'
from sales 
join stores 
on sales.store_id=stores.store_id
join products 
on sales.product_id=products.product_id
where sales.date between dateadd(Month,-6,(select max(date) from sales)) and (select max(date) from sales)
group by sales.store_id,stores.store_name,sales.date
order by sales.date desc

/*
Analysis NO. 3 

Need to analyse the trend in sales of last 6 months calcualted as per the last sales date recorded Plan for execution  we can add the the imprtant sales attributes

Method :- 

1. join the tables sales , stores and product 

2. Take sum of the sales unit column 

3. Multiply the unit and product_price columns and take sum of them  

4. use between fuction to stay in the range of last six months , we use dateadd function to add -6 months in the current date 

Outcome : -

Total units sold in last 6 months    Total revenue in last 6 months 
 
  Toys Guadalajara 1 - 86                   995.14
  Toys Monterrey 1   - 65                   924.35 
  Toys Guadalajara 2 - 8                    55.92
  Toys Saltillo 1    - 54                   633.46
*/

select date,datename(weekday,date), datepart(weekday,date) from sales

select stores.store_city, stores.store_location ,sum(units) as 'Total_un_sold',
case when Datename(weekday,sales.date) in ('Saturday','Sunday') then 'Weekend'
     Else 'Weekdays' End as 'Weekday_type'
from sales 
join stores
on sales.store_id=stores.store_id
where stores.store_location='Airport'
group by case when Datename(weekday,sales.date) in ('Saturday','Sunday') then 'Weekend'
     Else 'Weekdays' End,
	 stores.store_location ,stores.store_city

/*
Analysis NO. 4

what is the difference in Sales performance over the weekdays and weekends for each store location , it will also exhibits the sales performance during the
weekparts(weekday,weekends) to maintain the flow of inventory, revenue, and other administrative particulars

Method :-

1. join  the table sales and stores 

2. total the units columns of sales table

3. we use case and datename functions to check weather it is a weekend or a weekday , if it is saturday or sunday than weekend else weekday 

4. use where function to select store location (we have to choose each location specificlly for better undersatnding otherwise it is very difficult to analysis all 
locations at once)

Outcome : -

Store_location Airport has higher sales on weekdays in every city (This outcome is only for store_location = Airport )

Store_location Residential has higher sales on weekdays in every city (This outcome is only for store_location = Residential )
*/

with comp_sales as (select p.Product_category,year(s.date) as years, datepart(quarter,date) as 'quarterly' , sum(s.units) as 'total_un_sold'
from sales s
join products p 
on s.product_id=p.product_id
where s.date between (select min(s.date) from sales s) and (select max(s.date) from sales s)
group by p.product_category,datepart(quarter,s.date),year(s.date))
, Prev_sales as (select product_category, Quarterly, total_un_sold as prev_yr_un_sold
from comp_sales
where years=2022)

,crrnt_year_sales as (select product_category, Quarterly, total_un_sold as crrnt_yr_un_sold
from comp_sales
where years=2023)
select c.Product_category, c.Quarterly,c.crrnt_yr_un_sold,p.prev_yr_un_sold
from crrnt_year_sales C
join prev_sales p
on c.product_category=p.product_category and c.quarterly=p.quarterly
where c.crrnt_yr_un_sold>prev_yr_un_sold

select * from sales
where date='2023-09-30'

select dateadd(Month,-6,(select max(date) from sales))

/*

Analysis NO. 5

Determine the product categories that have shown a significant increase in sales compared to the same quarter last year or change as per sales

Method :-

1. we have to create a temporary dataset by joining sales and product tables 

2. add the units column of the sales table 

3. use between operator on date for keeping it in proper range

4. use groupby for arranging the product_category quarter wise 

5. create 2 more temporary dataset , one for previous year sales and one for current year sales 

6. join both the temporary datasets

7. apply condition that c.crrnt_yr_un_sold > prev_yr_un_sold 

Outcome as per analysis : -

Product_category	Quarterly	crrnt_yr_un_sold	prev_yr_un_sold
Art & Crafts			1			67855				15038
Art & Crafts			2			73632				22990
Art & Crafts			3			61575				28838
Games					1			47973				35998
Games					2			40395				39103
Sports & Outdoors		1			26369				21962
Sports & Outdoors		2			34322				31605
Toys					1			45095				37953
Toys					2			56672				48832
Toys					3			51199				29104

 */


