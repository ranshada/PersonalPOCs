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

CREATE TABLE movies (

    movie_id INTEGER DEFAULT nextval('movie_seq') PRIMARY KEY,
    movie_name varchar(100) NOT NULL,
    movie_length INTEGER ,
    movie_lang varchar(100),
    age_certificate varchar(20)
    release_date DATE

);


