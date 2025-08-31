// Valid migration for testing successful JS loading

export async function up(db) {
  await db.executeQuery({ sql: 'CREATE TABLE test_js (id INTEGER);', parameters: [] })
}

export async function down(db) {
  await db.executeQuery({ sql: 'DROP TABLE test_js;', parameters: [] })
}
