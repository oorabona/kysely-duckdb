-- Test migration without down section
-- migrate:up
CREATE TABLE test_table (id INTEGER);
INSERT INTO test_table VALUES (1);