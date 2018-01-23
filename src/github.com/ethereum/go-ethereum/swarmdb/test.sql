
-- this table test creates { string, int, float, blob }
create table t ( email text, age int, gender text, weight float, pwd blob, primary key (email), key (age), key(gender), key(weight) );

-- insert 200 records of string, int, float (a more elaborate test would have UTF-8 text and blob type)
insert into t (email, age, gender, weight) values ('t001@test.com', '1', 'F', 1.314159);
insert into t (email, age, gender, weight) values ('t002@test.com', '1', 'M', 1.414159);
insert into t (email, age, gender, weight) values ('t003@test.com', '2', 'F', 2.314159);
insert into t (email, age, gender, weight) values ('t004@test.com', '2', 'M', 2.414159);
...
insert into t (email, age, gender, weight) values ('t199@test.com', '199', 'F', 100.314159);
insert into t (email, age, gender, weight) values ('t200@test.com', '200', 'M', 100.414159);

-- cursor test: primary key equals on string
select * from t where email = 't001@test.com';
select * from t where email = 't1000@test.com';

-- cursor test: secondary key = on int
select * from t where age = 3;
select * from t where age != 3;

-- cursor test: secondary key = on string
select * from t where gender = 'F';
select * from t where gender != 'F';

-- cursor test: secondary key = on string with AND
select * from t where gender = 'F' and age = 3 order by email ASC;
select * from t where gender = 'F' and age = 1000 order by email ASC;
select * from t where gender = 'F' and email = 't001@test.com'  order by email ASC;

-- cursor test: primary key >= 
select * from t where email >= 't005@test.com' order by email ASC;

-- cursor test: primary key >= 
select * from t where email >= 't005@test.com' order by email DESC;

-- cursor test: primary key <= 
select * from t where email <= 't040@test.com' order by email ASC;

-- cursor test: primary key <= 
select * from t where email <= 't040@test.com' order by email DESC;

-- cursor test: primary key >= and test
select * from t where email >= 't005@test.com' and email <= 't010@test.com' order by email ASC;
select * from t where email >= 't005@test.com' and email <= 't010@test.com' order by email DESC;

-- cursor test: secondary key >=, < on int
select * from t where age >= 50 order by email ASC;
select * from t where age >= 50 order by email DESC;
select * from t where age < 50 order by email ASC;
select * from t where age < 50 order by email DESC;

-- cursor test: secondary key >=, < on float
select * from t where weight >= 50.0 order by email ASC;
select * from t where weight >= 50.0 order by email DESC;
select * from t where weight < 50.0 order by email ASC;
select * from t where weight < 50.0 order by email DESC;

-- cursor test: secondary key >=, < on int
select * from t where age >= 50 order by email ASC;
select * from t where age >= 50 order by email DESC;
select * from t where age < 50 order by email ASC;
select * from t where age < 50 order by email DESC;

-- cursor test: secondary key >= and < on int
select * from t where age >= 65 and age < 70 order by email ASC;
select * from t where age >= 65 and age < 70 order by email DESC;

-- cursor test: secondary key >= and < on float
select * from t where weight >= 65 and weight < 70 order by email ASC;
select * from t where weight >= 65 and weight < 70 order by email DESC;

-- cursor test: secondary key >= and < on string and int
select * from t where gender = 'F' and gender = 'M' order by email ASC;
select * from t where gender = 'F' and age = 65 order by email ASC;
select * from t where gender = 'F' and age >= 65 and age < 70 order by email ASC;

-- cursor test: in on string and int
select * from t where email in ('t018@test.com', 't021@test.com', 't065@test.com');
select * from t where email in ('t918@test.com', 't921@test.com', 't965@test.com');
select * from t where age in (18, 21, 65);
select * from t where age in (918, 921, 965);
select * from t where email in ('t001@test.com', 't002@test.com', 't003@test.com') and age in (1, 2, 3) and gender in ('F');
select * from t where email in ('t001@test.com', 't002@test.com', 't003@test.com') and age in (991, 992, 993) and gender in ('F');
select * from t where email in ('t901@test.com', 't902@test.com', 't903@test.com') and age in (1, 2, 3) and gender in ('F');

-- cursor test: like on string
select * from t where email like 't03%';
select * from t where email like 'x03%';




