-- Test migration with complex SQL statements for splitSqlStatements (lines 118-123)
-- migrate:up
CREATE TABLE test_complex (
    id INTEGER,
    data TEXT DEFAULT 'value;with;semicolons'
);
-- Comment with ; semicolon in it
INSERT INTO test_complex VALUES (1, 'another;test;value');; ; ;
UPDATE test_complex SET data = 'final;value' WHERE id = 1;

-- migrate:down
DROP TABLE test_complex;