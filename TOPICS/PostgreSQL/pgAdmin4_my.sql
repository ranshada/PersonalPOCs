CREATE SEQUENCE directors_director_id_seq
START WITH 1000  -- Start the sequence at 1000
INCREMENT BY 1  -- Increment by 5 each time
MINVALUE 1      -- Minimum value
MAXVALUE 1000000 -- Maximum value
CYCLE           -- Cycle back to the minimum value after reaching the maximum
CACHE 20        -- Cache 20 values in memory for faster access
;

CREATE TABLE directors 
(

director_id INTEGER DEFAULT nextval('directors_director_id_seq') PRIMARY KEY,
first_name varchar(100),
last_name varchar(40),
date_of_birth DATE,ac
create_date DATE,
update_date DATE

);



select * from directors;


CREATE SEQUENCE directors_director_id_seq
START WITH 1000  -- Start the sequence at 1000
INCREMENT BY 1  -- Increment by 5 each time
MINVALUE 1      -- Minimum value
MAXVALUE 1999 -- Maximum value
CYCLE           -- Cycle back to the minimum value after reaching the maximum
CACHE 20        -- Cache 20 values in memory for faster access
;


CREATE SEQUENCE movie_seq
START WITH 2000
INCREMENT BY 2
MINVALUE 1
MAXVALUE 2999
CYCLE
CACHE 20
;
CREATE TABLE movies (

    movie_id INTEGER DEFAULT nextval('movie_seq') PRIMARY KEY,
    movie_name varchar(100) NOT NULL,
    movie_length INTEGER ,
    movie_lang varchar(100),
    age_certificate varchar(20),
    release_date DATE,
	director_id INTEGER REFERENCES directors.director_id

);



ALTER TABLE movies 
ADD COLUMN director_id INTEGER REFERENCES directors (director_id);


CREATE SEQUENCE movies_revenue_seq
START WITH 3000
INCREMENT 4
MINVALUE 1
MAXVALUE 3999
CYCLE
CACHE 20;

CREATE TABLE movies_revenue (
revenue_id INTEGER PRIMARY KEY,
movie_id INTEGER REFERENCES movies (movie_id),
revenues_domestic NUMERIC(10,2),
revenues_international NUMERIC(10,2)

)

ALTER TABLE actors
RENAME COLUMN actior_id TO actor_id;

CREATE TABLE movies_actors(
movie_id INTEGER REFERENCES movies (movie_id),
actor_id INTEGER REFERENCES actors (actor_id),
PRIMARY KEY (movie_id, actor_id)

);











CREATE TABLE movies_revenues (
revenue_id 
)

CREATE TABLE t_tags (

id serial PRIMARY KEY,
tag text UNIQUE,
update_date TIMESTAMP DEFAULT NOW()
)

insert into t_tags(tag) VALUES ('PEN'),('PENCIL');

select * from movies;

select .*, count(*) as Total_movies from movies
group by movie_lang
order by count(*) desc

select movie_name, movie_lang, 
row_number() over (partition by movie_lang) as row_number,
avg(movie_length) over (partition by movie_lang) as avg_length,
count(*) over (partition by movie_lang) as total_movies
from movies
order by  row_number desc

group by movie_lang
order by count(*) desc

--find highest sal
with cte as (
select m.movie_name, mr.revenues_domestic, row_number() over ( order by mr.revenues_domestic desc ) 
as seque from movies_revenue mr
join movies m on mr.movie_id=m.movie_id 
where revenues_domestic is not null)
select * from cte where seque < 4

select * from movies m join movies_revenue mr on m.movie_id = mr.movie_id
where m.movie_name= 'Crouching Tiger Hidden Dragon'

--find duplicate records
select first_name , count(*) from actors
group by first_name
having count(*)>1;

with cte as(select first_name, last_name, 
row_number() over (partition by first_name ) as num
from actors)
select * from cte where num > 1;

where num > 1;

----find highest revenue in each movie 
with cte as (select m.movie_lang, m.movie_name, mr.revenues_domestic, mr.revenues_international ,
row_number() over (partition by movie_lang 
order by revenues_domestic  desc) as rank_num
from
movies_revenue mr join movies m on m.movie_id=mr.movie_id 
where 
revenues_domestic is not null)
select * from cte where rank_num=1;




---highest revenues in all categories
explain analyze
with cte as (select m.movie_lang, m.movie_name, mr.revenues_domestic, mr.revenues_international ,
row_number() over (order by revenues_domestic desc)  as ran
from
movies_revenue mr join movies m on m.movie_id=mr.movie_id 
where 
revenues_domestic is not null)
select * , case 
when revenues_domestic > 404.10 
then 'SUPPER HITTTTT' 
when revenues_domestic = 404.10 
then 'AVERAGE'
else 'UTTER FLOP' end as RESULT 
from cte where ran<=5
order by revenues_domestic  desc

explain for select * from movies 

EXPLAIN PLAN FOR
SELECT * FROM movies WHERE 1 = 2;


EXPLAIN 
SELECT * FROM movies;


---greater than 404.10 is supper hit , less than is utter flop, equals is average

select 
case 
when revenues_domestic > 404.10 
then 'SUPPER HITTTTT' 
when revenues_domestic = 404.10 
then 'AVERAGE'
else 'UTTER FLOP'





select 
first_name,
last_name,
gender,
count(gender) over (partition by gender)
from actors

--Using row number to find duplicate 

select a.actor_id, a.first_name, a.last_name, a.gender, count(gender) over (partition by gender)from actors a join movies_actors ma on
a.actor_id=ma.actor_id 

explain 

select a.* , count(*) from actors a join movies_actors ma 
on a.actor_id=ma.actor_id 
group by a.actor_id, a.first_name, a.last_name, a.gender, ma.movie_id;

ALTER table movies_actors 
add FOREIGN KEY (actor_id) REFERENCES actors(actor_id)
commit;
select * from actors where first_name='Jodie' and last_name='Foster'

INSERT INTO actors (first_name,last_name,gender,date_of_birth) VALUES
('Ranjith','Shada','M','1994-01-23')

select * from t_tags;


2025-02-11 14:54:12.342773

INSERT INTO t_tags(tag) VALUES ('PEN')
ON CONFLICT (tag) DO NOTHING;

create table movies_copy clone movies;
--to get current database and schema
select current_database()||'.'|| current_schema();
show tables;

SELECT substring('PostgreSQL', 5, 4); -- Result: Post


INSERT INTO t_tags(tag) VALUES ('PENCIL')
ON CONFLICT (tag) DO UPDATE set tag='PENCIL_UPDATED' , update_date =now()

INSERT INTO t_tags(tag) VALUES ('PENCIL')
ON CONFLICT (tag) DO UPDATE set tag=EXCLUDED.tag || '1', update_date =now()


select * from actors;

select first_name || ' ' || last_name as "Full Name" 
from actors -- returning first_name || last_name;

select 2 * 6 as Multiply;


create view my_view as 
with cte as (select m.movie_lang, m.movie_name, mr.revenues_domestic, mr.revenues_international ,
row_number() over (order by revenues_domestic desc)  as ran
from
movies_revenue mr join movies m on m.movie_id=mr.movie_id 
where 
revenues_domestic is not null)
select * , case 
when revenues_domestic > 404.10 
then 'SUPPER HITTTTT' 
when revenues_domestic = 404.10 
then 'AVERAGE'
else 'UTTER FLOP' end as RESULT 
from cte where ran<=5
order by revenues_domestic  desc;

select * from my_view where movie_name like 'S%';



--cte  --common table expressions---view will be persisted in the 
--database and available even after restarting session, 
--whereas cte are temporary , if we restart the data, we need to create cte as well

select * from mov

select * from movies;
select * from directors;

select to_char(release_date::timestamp,'yyyy-MM') as year, count(*) 
over (partition by to_char(release_date::timestamp,'yyyy'))from movies 
--where to_char(release_date::timestamp,'yyyy') = '2007'
order by 1 desc;

insert into directors 
(first_name,last_name,date_of_birth,nationality, create_date, update_date) 
values 
('Ranjith', 'Shada', '1994-01-23','Indian',NOW(), NOW())

INSERT INTO directors (first_name, last_name, date_of_birth, nationality, create_date, update_date) 
VALUES ('Ranjith', 'Shada', '1994-01-23', 'Indian', NOW(), NOW());


create table directors_not_directed_afilm 
as (
select d.director_id, d.first_name, d.last_name
from directors d
 LEFT OUTER JOIN movies m
on m.director_id=d.director_id
and to_char(date_of_birth::TIMESTAMP, 'yyyy-mm')='1994-01'
where m.director_id is null
and to_char(date_of_birth::TIMESTAMP, 'yyyy-mm')='1994-01'
);

create table directors_not_directed_afilm 
as (
select *
from directors d
NATURAL LEFT OUTER JOIN movies m


create table directors_not_directed_afilm 
as (
select
from directors d
NATURAL LEFT OUTER JOIN movies m
on m.director_id=d.director_id
and to_char(date_of_birth::TIMESTAMP, 'yyyy-mm')='1994-01'
where m.director_id is null
and to_char(date_of_birth::TIMESTAMP, 'yyyy-mm')='1994-01'
);


--
select to_char(release_date::timestamp,'yyyy-MM') as year, count(*) 
over (partition by to_char(release_date::timestamp,'yyyy-MM')),
count(*) over (partition by to_char(release_date::timestamp,'yyyy')) as total_movie_this_year
from movies 
--where to_char(release_date::timestamp,'yyyy') = '2007'
order by 1 desc;


select * from northorders;



create table rank_demo 
as
select order_date , 
count(*) over (partition by to_char(order_date::timestamp, 'yyyy')) as monthly_ordered_size,
freight,
sum(freight) over (partition by to_char(order_date::timestamp, 'yyyy-mm')) as total_freight,
rank() over (order by freight desc) as rank,
dense_rank() over (order by  freight desc)

from orders where to_char(order_date::timestamp, 'yyyy')='1996'
order by freight desc 
;

select rank, count(*) from rank_demo group by rank;


---ranking
select order_date , 
count(*) over (partition by to_char(order_date::timestamp, 'yyyy')) as yearly_ordered_size,
rank() over (partition by to_char(order_date::timestamp, 'yyyy-MM') order by order_date desc) as daily_ranks_orders,
freight,
sum(freight) over (partition by to_char(order_date::timestamp, 'yyyy-mm')) as total_freight,
rank() over (order by freight desc) as rank,
dense_rank() over (order by  freight desc)
from orders
order by order_date 
;


---filtering ranking
select * from (select order_date , 
count(*) over (partition by to_char(order_date::timestamp, 'yyyy')) as yearly_ordered_size,
rank() over (partition by to_char(order_date::timestamp, 'yyyy-MM') order by order_date desc) as daily_ranks_orders,
freight,
sum(freight) over (partition by to_char(order_date::timestamp, 'yyyy-mm')) as total_freight,
rank() over (order by freight desc) as rank,
dense_rank() over (order by  freight desc)
from orders
) as q
where daily_ranks_orders=1

