
-- Order 테이블 만들기
create table if not exists  ucdemoca.ucdemodb.order(
  order_id Int,
  user_id Int,
  category_id Int,
  menu_id Int,
  final_payment Int
);

insert into ucdemoca.ucdemodb.order values
(1,2,2,3, 1800), (2,3,1,2, 4800), (3,1,1,1, 5000), (4,3,1,1, 4000), (5,2,1,1, 4500), 
(6,1,3,2, 2000), (7,2,2,1, 2700), (8,2,3,1, 3600), (9,3,2,2, 1600), (10,2,1,3, 6300), 
(11,3,2,1, 2400), (12,3,1,2, 4800), (13,3,2,3, 1600), (14,2,2,2, 1800), (15,2,2,2, 1800), 
(16,1,1,1, 5000), (17,1,1,1, 5000), (18,1,3,3, 4000), (19,2,2,1, 2700), (20,3,3,1, 3200), 
(21,2,3,1, 3600), (22,3,2,3, 1600), (23,1,3,2, 2000), (24,1,3,2, 2000), (25,3,2,1, 2400), 
(26,2,3,1, 3600), (27,1,2,3, 2000), (28,3,1,3, 5600), (29,3,3,1, 3200), (30,2,3,3, 3600), 
(31,3,2,1, 2400), (32,1,3,3, 4000), (33,1,1,3, 7000), (34,1,3,1, 4000), (35,2,3,3, 3600), 
(36,3,3,2, 1600), (37,1,3,2, 2000), (38,2,1,1, 4500), (39,1,3,1, 4000), (40,3,2,1, 2400), 
(41,1,3,2, 2000), (42,1,2,2, 2000), (43,1,2,2, 2000), (44,3,2,1, 2400), (45,2,3,1, 3600), 
(46,2,3,1, 3600), (47,1,2,3, 2000), (48,1,3,3, 4000), (49,2,3,1, 3600), (50,2,1,3, 6300);

select * from ucdemoca.ucdemodb.order;

-- Category 테이블 만들기
create table if not exists  ucdemoca.ucdemodb.category(
  category_id Int,
  category_name String
);

insert into ucdemoca.ucdemodb.category values
(1,'Burger')
,(2,'Side')
,(3,'Dessert');

select * from ucdemoca.ucdemodb.category;

-- User 테이블 만들기
create table if not exists  ucdemoca.ucdemodb.user(
  user_id Int,
  grade String, 
  dis_ratio Float
);

insert into ucdemoca.ucdemodb.user values
(1, 'Bronze', 1.0), (2, 'SILVER', 0.9), (3, 'PLATINUM', 0.8);

select * from ucdemoca.ucdemodb.user;


-- Menu 테이블 만들기
create table if not exists  ucdemoca.ucdemodb.menu(
  category_id Int,
  menu_id Int,
  menu_name String,
  price Int
);

insert into ucdemoca.ucdemodb.menu values
(1,1,'Classic Burger',5000),
(1,2,'Bigmac Burger',6000),
(1,3,'Sanghai Burger',7000),
(2,1,'Franch Fries',3000),
(2,2,'Coleslaw',2000),
(2,3,'Cheese Sticks',2000),
(3,1,'McFlurry',4000),
(3,2,'Strawberry Cone',2000),
(3,3,'vanilla shake',4000);

select * from ucdemoca.ucdemodb.menu;


-- DW용 테이블 만들기
create table ucdemoca.ucdemodb.demodw as 
select a.order_id, b.user_id ,b.grade, c.category_name, 
       d.menu_name, d.price,b.dis_ratio, a.final_payment
  from ucdemoca.ucdemodb.order a
 inner join ucdemoca.ucdemodb.user b
    on a.user_id = b.user_id
 inner join ucdemoca.ucdemodb.category c
    on a.category_id = c.category_id
 inner join ucdemoca.ucdemodb.menu d
    on a.category_id = d.category_id and a.menu_id = d.menu_id;

-- DM용 테이블 만들기
-- group by grade
create table ucdemoca.ucdemodb.demodmgroupgrade as
select grade, count(grade) grade_cnt from ucdemoca.ucdemodb.demodw
 group by grade;

-- group by order_id
create table ucdemoca.ucdemodb.demodmgrouporder as
select order_id, count(order_id) as cnt_order from ucdemoca.ucdemodb.demodw
 group by order_id;

-- group by user_id, grade
create table ucdemoca.ucdemodb.demodmtotalpayment as
select user_id, grade, sum(price) price, sum(final_payment) as total_payment from ucdemoca.ucdemodb.demodw
 group by user_id, grade;

-- rael payment - price - total_payment 
create table ucdemoca.ucdemodb.demodmdiscountamount as
select price, total_payment, (price - total_payment) dis_amount 
from ucdemoca.ucdemodb.demodmtotalpayment;