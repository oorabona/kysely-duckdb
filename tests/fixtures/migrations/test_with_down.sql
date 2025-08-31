-- Test migration with both up and down sections for lines 75-80, 88-92
-- migrate:up
CREATE TABLE test_complete (id INTEGER, name TEXT);
INSERT INTO test_complete VALUES (1, 'test');
INSERT INTO test_complete VALUES (2, 'another');

-- migrate:down
DELETE FROM test_complete WHERE id = 2;
DELETE FROM test_complete WHERE id = 1;
DROP TABLE test_complete;