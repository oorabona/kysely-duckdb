// Invalid migration - no up function (should trigger error on lines 103-104)

export async function down(db) {
  // Only down function, missing up function
  await db.executeQuery({ sql: 'DROP TABLE test_table;', parameters: [] })
}
