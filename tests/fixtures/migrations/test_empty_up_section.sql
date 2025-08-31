-- Test migration with empty up section (after trim)
-- This should trigger error on line 70-71

-- migrate:up
   
   

-- migrate:down
DROP TABLE test_table;