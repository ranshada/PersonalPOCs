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

select * from t_tags;


2025-02-11 14:54:12.342773

INSERT INTO t_tags(tag) VALUES ('PEN')
ON CONFLICT (tag) DO NOTHING;

SELECT substring('PostgreSQL', 5, 4); -- Result: Post


INSERT INTO t_tags(tag) VALUES ('PENCIL')
ON CONFLICT (tag) DO UPDATE set tag='PENCIL_UPDATED' , update_date =now()

INSERT INTO t_tags(tag) VALUES ('PENCIL')
ON CONFLICT (tag) DO UPDATE set tag=EXCLUDED.tag || '1', update_date =now()


select * from actors;

select first_name || ' ' || last_name as "Full Name" 
from actors -- returning first_name || last_name;

select 2 * 6 as Multiply;