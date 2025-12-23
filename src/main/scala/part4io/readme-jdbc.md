in docker/postgres
docker compose up

docker exec -it rockthejvm-flink-postgres bash
psql -U docker
create database rtjvm;
\c rtjvm;
create table people(name varchar(40) not null, age int not null);
select * from people;