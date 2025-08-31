-- Test migration without migrate:up section
-- This should trigger error on line 62-63
CREATE TABLE test_table (id INTEGER);

-- migrate:down
DROP TABLE test_table;