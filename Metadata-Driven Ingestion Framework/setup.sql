CREATE DATABASE IF NOT EXISTS superstore_silver;
CREATE SCHEMA IF NOT EXISTS superstore_silver.silver;

CREATE DATABASE IF NOT EXISTS sample_superstore;
CREATE SCHEMA IF NOT EXISTS sample_superstore.superstore;

INSERT INTO sample_superstore.superstore.src_people 
(person, region, LOAD_DTE, OPERATION_FLG)
VALUES 
('Anna Andreadi', 'West', CURRENT_TIMESTAMP(), 'I'),
('Chuck Magee', 'East', CURRENT_TIMESTAMP(), 'I'),
('Kelly Williams', 'Central', CURRENT_TIMESTAMP(), 'I'),
('Cassandra Brandow', 'South', CURRENT_TIMESTAMP(), 'I')
