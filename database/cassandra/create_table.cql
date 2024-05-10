CREATE KEYSPACE cc6240 WITH REPLICATION = { 'class': 'NetworkTopologyStrategy', 'replication_factor' :1};

USE cc6240;

CREATE TABLE instructor (
    name text,
    dept_name text,
    salary double,
    primary key((name), dept_name)
);

CREATE TABLE student (
    name text,
    dept_name text,
    tot_cred tinyint,
    advisor text,
    primary key((name), dept_name)
);
