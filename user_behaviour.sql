use Taobao;
desc user_behavior;

select * from user_behavior limit 10;

# check the number of null values in each column
SELECT
    COUNT(*) - COUNT(user_id) AS null_user_id,
    COUNT(*) - COUNT(item_id) AS null_item_id,
    COUNT(*) - COUNT(category_id) AS null_category_id,
    COUNT(*) - COUNT(behavior_type) AS null_behavior_type,
    COUNT(*) - COUNT(timestamps) AS null_timestamps
FROM user_behavior;

select count(*) as total_entries from user_behavior;


# Check for duplicate entries
SELECT user_id, item_id, timestamps,
       COUNT(*) AS cnt
FROM user_behavior
GROUP BY user_id, item_id, timestamps
HAVING cnt > 1;

# add a new column to the table as id
alter table user_behavior add id int first;
select * from user_behavior limit 5;
alter table user_behavior modify id int primary key auto_increment;

SHOW INDEX FROM user_behavior;

# remove duplicate entries
SELECT COUNT(*)
FROM user_behavior
WHERE (user_id, item_id, timestamps) IN (
    SELECT user_id, item_id, timestamps
    FROM user_behavior
    GROUP BY user_id, item_id, timestamps
    HAVING COUNT(*) > 1
)
  AND id NOT IN (
    SELECT MIN(id)
    FROM user_behavior
    GROUP BY user_id, item_id, timestamps
    HAVING COUNT(*) > 1
);


show VARIABLES like '%_buffer%';
SET GLOBAL innodb_buffer_pool_size = 10 * 1024 * 1024 * 1024;
show VARIABLES like '%_buffer%';

ALTER TABLE user_behavior
    ADD COLUMN datetimes TIMESTAMP(0) DEFAULT NULL,
    ADD COLUMN dates DATE DEFAULT NULL,
    ADD COLUMN times TIME DEFAULT NULL,
    ADD COLUMN hours TINYINT(2) UNSIGNED DEFAULT NULL;

UPDATE user_behavior
SET
    datetimes = @dt := FROM_UNIXTIME(timestamps),
    dates = DATE(@dt),
    times = TIME(@dt),
    hours = HOUR(@dt);

select * from user_behavior limit 5;


# Check for NULL values in the new columns
CREATE INDEX idx_user_behavior_dates ON user_behavior(dates);
SELECT count(*) FROM user_behavior WHERE dates IS NULL;

delete from user_behavior where dates is null;
SELECT count(*) FROM user_behavior WHERE dates IS NULL;

select max(datetimes), min(datetimes) from user_behavior;

delete from user_behavior
where datetimes < '2017-11-25 00:00:00'
   or datetimes > '2017-12-03 23:59:59';

# Data overview
desc user_behavior;

select *
from user_behavior limit 5;

SELECT count(*)
from user_behavior;

# --------------------------------
create table temp_behavior like user_behavior;

insert into temp_behavior
select * from user_behavior limit 100000;

select * from temp_behavior;

select dates,
       count(*) as PV,
       count(distinct user_id) as UV,
       round(count(*) / count(distinct user_id), 2) as 'pv/uv'
from temp_behavior
where behavior_type = 'pv'
group by dates;

create table pv_uv_summary(
    dates char(10),
    pv int(9),
    uv int(9),
    pv_uv_ratio decimal(10, 1)
);

SHOW INDEX FROM user_behavior;
CREATE INDEX idx_behavior_dates ON user_behavior (behavior_type, dates, user_id);

# COUNT(DISTINCT user_id) is expensive to compute in MySQL
# and can be split into two subqueries to improve performance:
INSERT INTO pv_uv_summary
SELECT a.dates, a.PV, b.UV, ROUND(a.PV / b.UV, 2) AS pv_uv_ratio
FROM
    (SELECT
         dates, COUNT(*) AS PV
     FROM user_behavior
     WHERE behavior_type = 'pv'
     GROUP BY dates
     ) a
        JOIN
    (SELECT dates, COUNT(user_id) AS UV
     FROM (SELECT DISTINCT dates, user_id
           FROM user_behavior
           WHERE behavior_type = 'pv'
           ) t
     GROUP BY dates) b
    ON a.dates = b.dates;

select * from pv_uv_summary;

select * from user_behavior where dates is null;

select count(*) from temp_behavior;


select user_id,dates
from temp_behavior
group by user_id,dates;


SELECT a.user_id,
       a.dates AS date_a,
       b.user_id,
       b.dates AS date_b
FROM (
        SELECT DISTINCT user_id, dates
        FROM temp_behavior
    ) a
    JOIN (
        SELECT DISTINCT user_id, dates
        FROM temp_behavior
    ) b
    ON a.user_id = b.user_id
WHERE a.dates < b.dates;


create table retention_rate (
    dates char(10),
    retention_day1 float,
    retention_day3 float,
    retention_day7 float
);



INSERT INTO retention_rate (dates, retention_day1, retention_day3, retention_day7)
SELECT dates,
       SUM(DATE_ADD(dates, INTERVAL 1 DAY) = next_date_1) / COUNT(*) AS retention_day1,
       SUM(DATE_ADD(dates, INTERVAL 3 DAY) = next_date_3) / COUNT(*) AS retention_day3,
       SUM(DATE_ADD(dates, INTERVAL 7 DAY) = next_date_7) / COUNT(*) AS retention_day7
FROM (
         SELECT user_id, dates,
                LEAD(dates, 1) OVER (PARTITION BY user_id ORDER BY dates) AS next_date_1,
                LEAD(dates, 3) OVER (PARTITION BY user_id ORDER BY dates) AS next_date_3,
                LEAD(dates, 7) OVER (PARTITION BY user_id ORDER BY dates) AS next_date_7
         FROM (SELECT DISTINCT user_id, dates FROM user_behavior) t
     ) AS daily_active_users
GROUP BY dates;


select * from retention_rate
order by dates;

# Time Series Analysis--------------------------------
create table date_hour_behavior(
    dates char(10),
    hours char(2),
    pv int,
    cart int,
    fav int,
    buy int
);

insert into date_hour_behavior
SELECT dates, hours,
       SUM(CASE WHEN behavior_type = 'pv' THEN 1 ELSE 0 END) AS pv,
       SUM(CASE WHEN behavior_type = 'cart' THEN 1 ELSE 0 END) AS cart,
       SUM(CASE WHEN behavior_type = 'fav' THEN 1 ELSE 0 END) AS fav,
       SUM(CASE WHEN behavior_type = 'buy' THEN 1 ELSE 0 END) AS buy
FROM user_behavior
GROUP BY dates, hours
ORDER BY dates, hours;


select *
from date_hour_behavior;

# User Conversion Rate Analysis -------------------------
create table behavior_type_summary(
    behavior_type varchar(5),
    num int
);

insert into behavior_type_summary
select behavior_type,
       count(DISTINCT user_id) user_num
from user_behavior
group by behavior_type
order by behavior_type desc;

ALTER TABLE behavior_type_summary
    ADD COLUMN num_all int;

UPDATE behavior_type_summary bts
    JOIN (
        SELECT behavior_type, COUNT(*) AS num_all
        FROM user_behavior
        GROUP BY behavior_type
    ) ub ON bts.behavior_type = ub.behavior_type
SET bts.num_all = ub.num_all;

select * from behavior_type_summary;


# Behavior Path Analysis -------------------------
DROP VIEW IF EXISTS user_behavior_view;
create view user_behavior_view as
    select user_id,
           item_id,
           SUM(case when behavior_type = 'pv' then 1 else 0 end) AS pv,
           SUM(case when behavior_type = 'fav' then 1 else 0 end) AS fav,
           SUM(case when behavior_type = 'cart' then 1 else 0 end) AS cart,
           SUM(case when behavior_type = 'buy' then 1 else 0 end) AS buy
from user_behavior
group by user_id, item_id;

select * from user_behavior_view;

DROP VIEW IF EXISTS user_behavior_standard;
create view user_behavior_standard as
SELECT
    user_id,
    item_id,
    (pv > 0) AS Viewed,
    (fav > 0) AS Favorite,
    (cart > 0) AS Cart,
    (buy > 0) AS Buy
FROM user_behavior_view;

select * from user_behavior_standard;

DROP VIEW IF EXISTS user_behavior_path;

create view user_behavior_path as
select *,
       concat(Viewed, Favorite, Cart, Buy) PurchasePath
from user_behavior_standard as a
where a.Buy > 0;

select * from user_behavior_path limit 10;

DROP VIEW IF EXISTS path_count;
create view path_count as
select PurchasePath,
       count(*) quantities
from user_behavior_path
group by PurchasePath
order by quantities desc;

select * from path_count;

create table renhua(
    path_type char(4),
    description varchar(40)
);

insert into renhua values
      ('0001','Purchased_directly'),
      ('1001','Purchased_after_browsing'),
      ('0011','Purchased_after_adding'),
      ('1011','Purchased_after_browsing_additions'),
      ('0101','Purchased_after_browsing_additions'),
      ('1101','Purchased_after_browsing_favourites'),
      ('0111','Purchased_after_adding_favourites'),
      ('1111','Purchased_after_browsing_favourites');

select * from renhua;


select *
from path_count p
join renhua r
on p.PurchasePath = r.path_type
order by quantities desc;


create table path_result
(
    path_type char(4),
    description varchar(40),
    num int);

insert into path_result
select path_type,
       description,
       quantities
from path_count p
join renhua r
on p.PurchasePath = r.path_type
order by quantities desc;

select * from path_result;

select sum(buy)
from user_behavior_view
where buy>0 and fav=0 and cart=0;


# RFM model ----------------
select user_id,
       count(user_id) as Frequency,
       max(dates) as LastBuyDate
from user_behavior
where behavior_type='buy'
group by user_id
order by 2 desc, 3 desc;


drop table if exists rfm_model;
create table rfm_model(
    user_id int,
    frequency int,
    recent char(10)
);

insert into rfm_model
select user_id,
      count(user_id) as frequency,
      max(dates) as recent
from user_behavior
where behavior_type='buy'
group by user_id
order by 2 desc, 3 desc;

select * from rfm_model;


alter table rfm_model add column fscore int;
update rfm_model
set fscore =
    case
         when frequency >= 100 then 5
         when frequency between 50 and 99 then 4
         when frequency between 20 and 49 then 3
         when frequency between 5 and 20 then 2
         else 1
    end;

alter table rfm_model add column rscore int;
update rfm_model
set rscore =
    case
         when recent = '2017-12-03' then 5
         when recent in ('2017-12-01','2017-12-02') then 4
         when recent in ('2017-11-29','2017-11-30') then 3
         when recent in ('2017-11-27','2017-11-28') then 2
         else 1
    end;

select * from rfm_model;


SET @f_avg = NULL;
SET @r_avg = NULL;

SELECT AVG(IFNULL(fscore, 0)), AVG(IFNULL(rscore, 0))
INTO @f_avg, @r_avg
FROM rfm_model;

alter table rfm_model add column class varchar(40);
update rfm_model
set class =
    case
        when fscore>@f_avg and rscore>@r_avg then 'Valuable'
        when fscore>@f_avg and rscore<@r_avg then 'Keep'
        when fscore<@f_avg and rscore>@r_avg then 'Develop'
        when fscore<@f_avg and rscore<@r_avg then 'Retain'
        else 'Uncertain'
    end;

select class,
       count(user_id) from rfm_model
group by class;



# Classification of goods by popularity ----------------
drop table if exists popular_categories;
drop table if exists popular_items;
drop table if exists popular_cateitems;

create table popular_categories(
    category_id int,
    pv int
                               );
create table popular_items(
    item_id int,
    v int
                          );
create table popular_cate_items(
    category_id int,
    item_id int,
    pv int
                              );

insert into popular_categories
SELECT category_id,
       SUM(behavior_type = 'pv') AS pv_count
FROM user_behavior
GROUP BY category_id
ORDER BY pv_count DESC
LIMIT 10;

insert into popular_items
SELECT item_id,
       SUM(behavior_type = 'pv') AS pv_count
FROM user_behavior
GROUP BY item_id
ORDER BY pv_count DESC
LIMIT 10;


INSERT INTO popular_cate_items (category_id, item_id, pv)
SELECT category_id, item_id, CategoryProductViews
FROM (
         SELECT category_id, item_id,
                SUM(behavior_type = 'pv') AS CategoryProductViews,
                RANK() OVER (PARTITION BY category_id ORDER BY SUM(behavior_type = 'pv') DESC) AS r
         FROM user_behavior
         GROUP BY category_id, item_id
     ) RankedItems
WHERE r = 1
ORDER BY CategoryProductViews DESC
LIMIT 10;


select * from popular_categories;
select * from popular_items;
select * from popular_cate_items;


# ----Product Conversion Rate Analysis
drop table if exists item_detail;
CREATE TABLE item_detail (
                             item_id INT,
                             pv INT,
                             fav INT,
                             cart INT,
                             buy INT,
                             user_buy_rate FLOAT
);

INSERT INTO item_detail
SELECT item_id,
       SUM(IF(behavior_type = 'pv', 1, 0)) AS pv,
       SUM(IF(behavior_type = 'fav', 1, 0)) AS fav,
       SUM(IF(behavior_type = 'cart', 1, 0)) AS cart,
       SUM(IF(behavior_type = 'buy', 1, 0)) AS buy,
       COUNT(DISTINCT IF(behavior_type = 'buy', user_id, NULL)) / COALESCE(COUNT(DISTINCT user_id), 1) AS conversion_rate
FROM user_behavior
GROUP BY item_id
ORDER BY conversion_rate DESC;

SELECT * FROM item_detail;

drop table if exists category_detail;
CREATE TABLE category_detail (
                                 category_id INT,
                                 pv INT,
                                 fav INT,
                                 cart INT,
                                 buy INT,
                                 user_buy_rate FLOAT
);

INSERT INTO category_detail
SELECT category_id,
       SUM(IF(behavior_type = 'pv', 1, 0)) AS pv,
       SUM(IF(behavior_type = 'fav', 1, 0)) AS fav,
       SUM(IF(behavior_type = 'cart', 1, 0)) AS cart,
       SUM(IF(behavior_type = 'buy', 1, 0)) AS buy,
       COUNT(DISTINCT IF(behavior_type = 'buy', user_id, NULL)) / COALESCE(COUNT(DISTINCT user_id), 1) AS conversion_rate
FROM user_behavior
GROUP BY category_id
ORDER BY conversion_rate DESC;

SELECT * FROM category_detail;