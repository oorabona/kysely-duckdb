-- Test migration without down section to trigger error on lines 82-86
-- migrate:up
CREATE TABLE test_no_down (id INTEGER);
INSERT INTO test_no_down VALUES (42);