-- Catalog 생성
create catalog if not exists ucdemoca;


-- Database(=Schema) 생성
create database if not exists ucdemoca.ucdemodb;

-- table 생성
create table if not exists  ucdemoca.ucdemodb.dummytable(
  id Int,
  name String
);

-- insert into data
insert into ucdemoca.ucdemodb.dummytable values
(1,'Dummy1')
,(2,'Dummy2')
,(3,'Dummy3');


-- select into data
insert into ucdemoca.ucdemodb.dummytable2 as
select * from ucdemoca.ucdemodb.dummytable


