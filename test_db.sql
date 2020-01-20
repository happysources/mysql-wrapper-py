
CREATE DATABASE IF NOT EXISTS test_db CHARSET utf8; 

GRANT select,insert,update,delete ON test_db.*  TO test_user@'%' IDENTIFIED BY 'test_passwd';

flush privileges;

USE test_db;

DROP TABLE IF EXISTS test_table;
CREATE TABLE test_table (
	id INT(10) unsigned PRIMARY KEY AUTO_INCREMENT,
	value_int int(10) unsigned not null,
	value_str VARCHAR(20) not null
);

INSERT INTO test_table VALUES (0, 101, 'one');
INSERT INTO test_table VALUES (0, 102, 'two');
INSERT INTO test_table VALUES (0, 103, 'three');
INSERT INTO test_table VALUES (0, 104, 'four');
INSERT INTO test_table VALUES (0, 105, 'five');
INSERT INTO test_table VALUES (0, 106, 'six');
