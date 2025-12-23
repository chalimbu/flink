in docker/cassandra

docker compose up
./cql.sh

create keyspace if not exists rtjvm with replication = {'class':'SimpleStrategy', 'replication_factor':'1'};
create table if not exists rtjvm.people(name text, age int, primary key(name));
select * from rtjvm.people;
run app
select * from rtjvm.people;