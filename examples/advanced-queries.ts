/**
 * Advanced Queries Example
 * 
 * This example demonstrates complex SQL patterns and advanced query
 * techniques using kysely-duckdb, including CTEs, window functions,
 * recursive queries, and analytical functions.
 */

import { Kysely, sql } from 'kysely'
import { DuckDBInstance } from '@duckdb/node-api'
import { DuckDbDialect } from '@oorabona/kysely-duckdb'

// Database schema for advanced query examples
interface AdvancedSchema {
  employees: {
    id: number
    name: string
    department: string
    manager_id: number | null
    salary: number
    hire_date: string | Date
    location: string
  }
  sales: {
    id: number
    employee_id: number
    product: string
    amount: number
    sale_date: string | Date
    quarter: string
  }
  departments: {
    id: number
    name: string
    budget: number
    head_id: number | null
  }
  projects: {
    id: number
    name: string
    department_id: number
    start_date: string | Date
    end_date: string | Date | null
    status: 'planning' | 'active' | 'completed' | 'cancelled'
  }
  project_assignments: {
    id: number
    project_id: number
    employee_id: number
    role: string
    allocation_percent: number
    start_date: string | Date
    end_date: string | Date | null
  }
}

async function createSampleData(db: Kysely<AdvancedSchema>) {
  console.log('📊 Creating comprehensive sample data...')
  
  // Create departments
  await db.schema
    .createTable('departments')
    .addColumn('id', 'integer', col => col.primaryKey())
    .addColumn('name', 'varchar(100)', col => col.notNull())
    .addColumn('budget', sql`decimal(15,2)`, col => col.notNull())
    .addColumn('head_id', 'integer')
    .execute()

  await db.insertInto('departments').values([
    { id: 1, name: 'Engineering', budget: 2500000, head_id: null },
    { id: 2, name: 'Sales', budget: 1200000, head_id: null },
    { id: 3, name: 'Marketing', budget: 800000, head_id: null },
    { id: 4, name: 'HR', budget: 600000, head_id: null },
    { id: 5, name: 'Finance', budget: 400000, head_id: null }
  ]).execute()

  // Create employees
  await db.schema
    .createTable('employees')
    .addColumn('id', 'integer', col => col.primaryKey())
    .addColumn('name', 'varchar(255)', col => col.notNull())
    .addColumn('department', 'varchar(100)', col => col.notNull())
    .addColumn('manager_id', 'integer')
    .addColumn('salary', sql`decimal(10,2)`, col => col.notNull())
    .addColumn('hire_date', 'date', col => col.notNull())
    .addColumn('location', 'varchar(100)', col => col.notNull())
    .execute()

  // Insert employees without null manager_id first
  await db.insertInto('employees').values([
    { id: 2, name: 'Bob Smith', department: 'Engineering', manager_id: 1, salary: 120000, hire_date: '2020-03-10', location: 'San Francisco' },
    { id: 3, name: 'Carol Davis', department: 'Engineering', manager_id: 1, salary: 110000, hire_date: '2021-05-20', location: 'New York' },
    { id: 4, name: 'David Wilson', department: 'Engineering', manager_id: 2, salary: 95000, hire_date: '2022-01-10', location: 'Remote' },
    { id: 6, name: 'Frank Miller', department: 'Sales', manager_id: 5, salary: 85000, hire_date: '2021-02-15', location: 'Los Angeles' },
    { id: 7, name: 'Grace Lee', department: 'Sales', manager_id: 5, salary: 90000, hire_date: '2021-08-20', location: 'Chicago' },
    { id: 9, name: 'Ivy Taylor', department: 'Marketing', manager_id: 8, salary: 75000, hire_date: '2022-03-15', location: 'Austin' }
  ]).execute()
  
  // Insert employees with null manager_id separately
  await db.insertInto('employees').values([
    { id: 1, name: 'Alice Johnson', department: 'Engineering', manager_id: sql`NULL::integer`, salary: 150000, hire_date: '2020-01-15', location: 'New York' },
    { id: 5, name: 'Emma Brown', department: 'Sales', manager_id: sql`NULL::integer`, salary: 130000, hire_date: '2019-06-01', location: 'Chicago' },
    { id: 8, name: 'Henry Chen', department: 'Marketing', manager_id: sql`NULL::integer`, salary: 110000, hire_date: '2020-09-01', location: 'Austin' },
    { id: 10, name: 'Jack Anderson', department: 'HR', manager_id: sql`NULL::integer`, salary: 95000, hire_date: '2019-11-01', location: 'New York' }
  ]).execute()

  // Update department heads
  await db.updateTable('departments').set({ head_id: 1 }).where('name', '=', 'Engineering').execute()
  await db.updateTable('departments').set({ head_id: 5 }).where('name', '=', 'Sales').execute()
  await db.updateTable('departments').set({ head_id: 8 }).where('name', '=', 'Marketing').execute()
  await db.updateTable('departments').set({ head_id: 10 }).where('name', '=', 'HR').execute()

  // Create sales data
  await db.schema
    .createTable('sales')
    .addColumn('id', 'integer', col => col.primaryKey())
    .addColumn('employee_id', 'integer', col => col.notNull())
    .addColumn('product', 'varchar(100)', col => col.notNull())
    .addColumn('amount', sql`decimal(10,2)`, col => col.notNull())
    .addColumn('sale_date', 'date', col => col.notNull())
    .addColumn('quarter', 'varchar(10)', col => col.notNull())
    .execute()

  const salesData = [
    { id: 1, employee_id: 5, product: 'Enterprise License', amount: 50000, sale_date: '2024-01-15', quarter: 'Q1-2024' },
    { id: 2, employee_id: 6, product: 'Professional License', amount: 15000, sale_date: '2024-01-20', quarter: 'Q1-2024' },
    { id: 3, employee_id: 7, product: 'Consultation', amount: 8000, sale_date: '2024-02-05', quarter: 'Q1-2024' },
    { id: 4, employee_id: 5, product: 'Enterprise License', amount: 75000, sale_date: '2024-02-12', quarter: 'Q1-2024' },
    { id: 5, employee_id: 6, product: 'Support Package', amount: 12000, sale_date: '2024-03-01', quarter: 'Q1-2024' },
    { id: 6, employee_id: 7, product: 'Training', amount: 25000, sale_date: '2024-04-10', quarter: 'Q2-2024' },
    { id: 7, employee_id: 5, product: 'Enterprise License', amount: 100000, sale_date: '2024-05-15', quarter: 'Q2-2024' },
    { id: 8, employee_id: 6, product: 'Professional License', amount: 18000, sale_date: '2024-06-20', quarter: 'Q2-2024' }
  ]

  await db.insertInto('sales').values(salesData).execute()

  // Create projects
  await db.schema
    .createTable('projects')
    .addColumn('id', 'integer', col => col.primaryKey())
    .addColumn('name', 'varchar(255)', col => col.notNull())
    .addColumn('department_id', 'integer', col => col.notNull())
    .addColumn('start_date', 'date', col => col.notNull())
    .addColumn('end_date', 'date')
    .addColumn('status', 'varchar(20)', col => col.notNull())
    .execute()

  await db.insertInto('projects').values([
    { id: 1, name: 'New API Platform', department_id: 1, start_date: '2024-01-01', end_date: sql`NULL::date`, status: 'active' },
    { id: 2, name: 'Mobile App Redesign', department_id: 1, start_date: '2023-10-01', end_date: '2024-02-28', status: 'completed' },
    { id: 3, name: 'Q2 Marketing Campaign', department_id: 3, start_date: '2024-03-01', end_date: '2024-06-30', status: 'active' },
    { id: 4, name: 'Sales Process Optimization', department_id: 2, start_date: '2024-02-15', end_date: sql`NULL::date`, status: 'active' },
    { id: 5, name: 'HR System Upgrade', department_id: 4, start_date: '2024-01-15', end_date: '2024-04-30', status: 'completed' }
  ]).execute()

  // Create project assignments
  await db.schema
    .createTable('project_assignments')
    .addColumn('id', 'integer', col => col.primaryKey())
    .addColumn('project_id', 'integer', col => col.notNull())
    .addColumn('employee_id', 'integer', col => col.notNull())
    .addColumn('role', 'varchar(100)', col => col.notNull())
    .addColumn('allocation_percent', 'integer', col => col.notNull())
    .addColumn('start_date', 'date', col => col.notNull())
    .addColumn('end_date', 'date')
    .execute()

  await db.insertInto('project_assignments').values([
    { id: 1, project_id: 1, employee_id: 1, role: 'Project Lead', allocation_percent: 80, start_date: '2024-01-01', end_date: null },
    { id: 2, project_id: 1, employee_id: 2, role: 'Senior Developer', allocation_percent: 100, start_date: '2024-01-01', end_date: null },
    { id: 3, project_id: 1, employee_id: 3, role: 'Developer', allocation_percent: 75, start_date: '2024-01-15', end_date: null },
    { id: 4, project_id: 2, employee_id: 2, role: 'Lead Developer', allocation_percent: 100, start_date: '2023-10-01', end_date: '2024-02-28' },
    { id: 5, project_id: 2, employee_id: 4, role: 'UI Developer', allocation_percent: 100, start_date: '2023-11-01', end_date: '2024-02-28' },
    { id: 6, project_id: 3, employee_id: 8, role: 'Campaign Manager', allocation_percent: 60, start_date: '2024-03-01', end_date: '2024-06-30' },
    { id: 7, project_id: 3, employee_id: 9, role: 'Content Creator', allocation_percent: 80, start_date: '2024-03-01', end_date: '2024-06-30' },
    { id: 8, project_id: 4, employee_id: 5, role: 'Sales Consultant', allocation_percent: 40, start_date: '2024-02-15', end_date: null },
    { id: 9, project_id: 5, employee_id: 10, role: 'System Administrator', allocation_percent: 90, start_date: '2024-01-15', end_date: '2024-04-30' }
  ]).execute()

  console.log('✅ Sample data created')
}

async function demonstrateCTEs(db: Kysely<AdvancedSchema>) {
  console.log('\n🔗 Common Table Expressions (CTEs)\n')

  // 1. Basic CTE - Department statistics
  console.log('1. Department statistics with CTE:')
  
  const departmentStats = await db
    .with('dept_stats', (db) => 
      db.selectFrom('employees')
        .groupBy('department')
        .select([
          'department',
          eb => eb.fn.count('id').as('employee_count'),
          eb => eb.fn.avg('salary').as('avg_salary'),
          eb => eb.fn.sum('salary').as('total_salary')
        ])
    )
    .selectFrom('dept_stats')
    .select([
      'department',
      'employee_count',
      eb => sql<number>`ROUND(${eb.ref('avg_salary')}, 2)`.as('avg_salary'),
      'total_salary'
    ])
    .orderBy('total_salary', 'desc')
    .execute()

  console.table(departmentStats)

  // 2. Multiple CTEs - Sales performance analysis
  console.log('\n2. Sales performance with multiple CTEs:')
  
  const salesAnalysis = await db
    .with('quarterly_sales', (db) =>
      db.selectFrom('sales')
        .groupBy(['quarter', 'employee_id'])
        .select([
          'quarter',
          'employee_id',
          eb => eb.fn.sum('amount').as('quarterly_total'),
          eb => eb.fn.count('id').as('sale_count')
        ])
    )
    .with('employee_performance', (db) =>
      db.selectFrom('quarterly_sales')
        .innerJoin('employees', 'employees.id', 'quarterly_sales.employee_id')
        .select([
          'employees.name',
          'quarterly_sales.quarter',
          'quarterly_sales.quarterly_total',
          'quarterly_sales.sale_count',
          eb => sql<number>`ROUND(${eb.ref('quarterly_sales.quarterly_total')} / ${eb.ref('quarterly_sales.sale_count')}, 2)`.as('avg_sale_value')
        ])
    )
    .selectFrom('employee_performance')
    .selectAll()
    .orderBy('quarter', 'asc')
    .orderBy('quarterly_total', 'desc')
    .execute()

  console.table(salesAnalysis)
}

async function demonstrateWindowFunctions(db: Kysely<AdvancedSchema>) {
  console.log('\n🪟 Window Functions\n')

  // 1. Ranking functions
  console.log('1. Employee salary rankings by department:')
  
  const salaryRankings = await db
    .selectFrom('employees')
    .select([
      'name',
      'department',
      'salary',
      eb => sql<number>`ROW_NUMBER() OVER (PARTITION BY ${eb.ref('department')} ORDER BY ${eb.ref('salary')} DESC)`.as('dept_rank'),
      eb => sql<number>`DENSE_RANK() OVER (ORDER BY ${eb.ref('salary')} DESC)`.as('company_rank'),
      eb => sql<number>`PERCENT_RANK() OVER (PARTITION BY ${eb.ref('department')} ORDER BY ${eb.ref('salary')})`.as('percentile')
    ])
    .orderBy('department', 'asc')
    .orderBy('salary', 'desc')
    .execute()

  console.table(salaryRankings)

  // 2. Running totals and moving averages
  console.log('\n2. Sales running totals and moving averages:')
  
  const salesTrends = await db
    .selectFrom('sales')
    .innerJoin('employees', 'employees.id', 'sales.employee_id')
    .select([
      'employees.name',
      'sales.sale_date',
      'sales.amount',
      eb => sql<number>`SUM(${eb.ref('sales.amount')}) OVER (PARTITION BY ${eb.ref('sales.employee_id')} ORDER BY ${eb.ref('sales.sale_date')})`.as('running_total'),
      eb => sql<number>`AVG(${eb.ref('sales.amount')}) OVER (PARTITION BY ${eb.ref('sales.employee_id')} ORDER BY ${eb.ref('sales.sale_date')} ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)`.as('moving_avg_3'),
      eb => sql<number>`LAG(${eb.ref('sales.amount')}, 1) OVER (PARTITION BY ${eb.ref('sales.employee_id')} ORDER BY ${eb.ref('sales.sale_date')})`.as('previous_sale'),
      eb => sql<number>`LEAD(${eb.ref('sales.amount')}, 1) OVER (PARTITION BY ${eb.ref('sales.employee_id')} ORDER BY ${eb.ref('sales.sale_date')})`.as('next_sale')
    ])
    .orderBy('employees.name')
    .orderBy('sales.sale_date')
    .execute()

  console.table(salesTrends)

  // 3. Analytical functions with partitioning
  console.log('\n3. Project workload analysis:')
  
  const workloadAnalysis = await db
    .selectFrom('project_assignments')
    .innerJoin('employees', 'employees.id', 'project_assignments.employee_id')
    .innerJoin('projects', 'projects.id', 'project_assignments.project_id')
    .select([
      'employees.name',
      'projects.name as project',
      'project_assignments.allocation_percent',
      eb => sql<number>`SUM(${eb.ref('project_assignments.allocation_percent')}) OVER (PARTITION BY ${eb.ref('employees.id')})`.as('total_allocation'),
      eb => sql<number>`COUNT(*) OVER (PARTITION BY ${eb.ref('employees.id')})`.as('project_count'),
      eb => sql<number>`AVG(${eb.ref('project_assignments.allocation_percent')}) OVER (PARTITION BY ${eb.ref('employees.department')})`.as('dept_avg_allocation')
    ])
    .where('projects.status', '=', 'active')
    .orderBy('employees.name')
    .execute()

  console.table(workloadAnalysis)
}

async function demonstrateRecursiveQueries(db: Kysely<AdvancedSchema>) {
  console.log('\n🔄 Recursive Queries\n')

  // 1. Organization hierarchy
  console.log('1. Employee hierarchy (managers and reports):')
  
  const hierarchy = await sql<{
    employee_id: number
    employee_name: string
    manager_id: number | null
    manager_name: string | null
    level: number
    path: string
  }>`
    WITH RECURSIVE employee_hierarchy AS (
      -- Base case: top-level managers
      SELECT 
        id as employee_id,
        name as employee_name,
        manager_id,
        CAST(NULL AS VARCHAR) as manager_name,
        0 as level,
        name as path
      FROM employees 
      WHERE manager_id IS NULL
      
      UNION ALL
      
      -- Recursive case: employees with managers
      SELECT 
        e.id as employee_id,
        e.name as employee_name,
        e.manager_id,
        eh.employee_name as manager_name,
        eh.level + 1 as level,
        eh.path || ' > ' || e.name as path
      FROM employees e
      INNER JOIN employee_hierarchy eh ON e.manager_id = eh.employee_id
    )
    SELECT * FROM employee_hierarchy
    ORDER BY level, employee_name
  `.execute(db)

  console.table(hierarchy.rows)

  // 2. Department budget rollup
  console.log('\n2. Department budget analysis with subordinates:')
  
  const budgetRollup = await sql<{
    department: string
    direct_reports: number
    total_team_size: number
    direct_salary_cost: number
    total_salary_cost: number
  }>`
    WITH RECURSIVE team_hierarchy AS (
      -- Managers
      SELECT 
        e.id as employee_id,
        e.name,
        e.department,
        e.salary,
        e.manager_id,
        0 as level
      FROM employees e
      WHERE e.manager_id IS NULL
      
      UNION ALL
      
      -- Their reports
      SELECT 
        e.id as employee_id,
        e.name,
        th.department, -- Inherit manager's department for rollup
        e.salary,
        e.manager_id,
        th.level + 1 as level
      FROM employees e
      INNER JOIN team_hierarchy th ON e.manager_id = th.employee_id
    ),
    department_rollup AS (
      SELECT 
        department,
        COUNT(CASE WHEN level = 1 THEN 1 END) as direct_reports,
        COUNT(*) as total_team_size,
        SUM(CASE WHEN level = 1 THEN salary ELSE 0 END) as direct_salary_cost,
        SUM(salary) as total_salary_cost
      FROM team_hierarchy
      GROUP BY department
    )
    SELECT * FROM department_rollup
    ORDER BY total_salary_cost DESC
  `.execute(db)

  console.table(budgetRollup.rows)
}

async function demonstrateAdvancedJoins(db: Kysely<AdvancedSchema>) {
  console.log('\n🔗 Advanced Joins and Subqueries\n')

  // 1. Correlated subqueries
  console.log('1. Employees earning above department average:')
  
  const aboveAverage = await db
    .selectFrom('employees as e1')
    .select((eb) => [
      'e1.name',
      'e1.department',
      'e1.salary',
      sql<number>`(SELECT AVG(salary) FROM employees e2 WHERE e2.department = e1.department)`.as('dept_avg'),
      sql<number>`ROUND(${eb.ref('e1.salary')} - (SELECT AVG(salary) FROM employees e2 WHERE e2.department = e1.department), 2)`.as('above_avg_by')
    ])
    .where(
      'e1.salary',
      '>',
      sql<number>`(SELECT AVG(salary) FROM employees e2 WHERE e2.department = e1.department)`
    )
    .orderBy('e1.department', 'asc')
    .orderBy('above_avg_by', 'desc')
    .execute()

  console.table(aboveAverage)

  // 2. Complex joins with project aggregation  
  console.log('\n2. Employees with active project details:')
  
  const employeeProjects = await db
    .selectFrom('employees')
    .innerJoin('project_assignments', 'project_assignments.employee_id', 'employees.id')
    .innerJoin('projects', 'projects.id', 'project_assignments.project_id')
    .select([
      'employees.name',
      'employees.department',
      'projects.name as project_name',
      'project_assignments.role',
      'project_assignments.allocation_percent',
      'projects.status'
    ])
    .where('projects.status', '=', 'active')
    .orderBy('employees.name')
    .execute()

  console.log('Employee Projects:')
  console.table(employeeProjects)

  // 3. Self-joins for peer comparison
  console.log('\n3. Employee peer salary comparison:')
  
  const peerComparison = await db
    .selectFrom('employees as e1')
    .innerJoin('employees as e2', (join) =>
      join.onRef('e1.department', '=', 'e2.department')
          .onRef('e1.id', '!=', 'e2.id')
    )
    .select([
      'e1.name as employee',
      'e1.salary as employee_salary',
      'e2.name as peer',
      'e2.salary as peer_salary',
      eb => sql<number>`${eb.ref('e1.salary')} - ${eb.ref('e2.salary')}`.as('salary_diff')
    ])
    .where('e1.id', '=', 2) // Focus on one employee for clarity
    .orderBy('salary_diff', 'desc')
    .execute()

  console.table(peerComparison)
}

async function demonstrateAnalyticalFunctions(db: Kysely<AdvancedSchema>) {
  console.log('\n📊 Advanced Analytics and Aggregations\n')

  // 1. Time series analysis
  console.log('1. Monthly sales trends with growth rates:')
  
  const salesTrends = await sql<{
    month: string
    total_sales: number
    sale_count: number
    avg_sale_size: number
    month_over_month_growth: number
    cumulative_sales: number
  }>`
    WITH monthly_sales AS (
      SELECT 
        DATE_TRUNC('month', sale_date) as month,
        SUM(amount) as total_sales,
        COUNT(*) as sale_count,
        AVG(amount) as avg_sale_size
      FROM sales
      GROUP BY DATE_TRUNC('month', sale_date)
    ),
    sales_with_growth AS (
      SELECT 
        month,
        total_sales,
        sale_count,
        ROUND(avg_sale_size, 2) as avg_sale_size,
        ROUND(
          (total_sales - LAG(total_sales) OVER (ORDER BY month)) / 
          NULLIF(LAG(total_sales) OVER (ORDER BY month), 0) * 100, 
          2
        ) as month_over_month_growth,
        SUM(total_sales) OVER (ORDER BY month) as cumulative_sales
      FROM monthly_sales
    )
    SELECT 
      strftime('%Y-%m', month) as month,
      total_sales,
      sale_count,
      avg_sale_size,
      month_over_month_growth,
      cumulative_sales
    FROM sales_with_growth
    ORDER BY month
  `.execute(db)

  console.table(salesTrends.rows)

  // 2. Advanced statistical analysis
  console.log('\n2. Salary distribution statistics by department:')
  
  const salaryStats = await sql<{
    department: string
    employee_count: number
    min_salary: number
    q1_salary: number
    median_salary: number
    q3_salary: number
    max_salary: number
    std_dev: number
    coefficient_of_variation: number
  }>`
    SELECT 
      department,
      COUNT(*) as employee_count,
      MIN(salary) as min_salary,
      PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY salary) as q1_salary,
      PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) as median_salary,
      PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY salary) as q3_salary,
      MAX(salary) as max_salary,
      ROUND(STDDEV(salary), 2) as std_dev,
      ROUND(STDDEV(salary) / AVG(salary) * 100, 2) as coefficient_of_variation
    FROM employees
    GROUP BY department
    ORDER BY median_salary DESC
  `.execute(db)

  console.table(salaryStats.rows)

  // 3. Complex business intelligence query
  console.log('\n3. Department performance scorecard:')
  
  const scorecard = await sql<{
    department: string
    headcount: number
    avg_tenure_years: number
    total_budget: number
    total_salary_cost: number
    budget_utilization: number
    active_projects: number
    total_project_allocation: number
    avg_employee_utilization: number
    performance_score: number
  }>`
    WITH department_metrics AS (
      SELECT 
        d.name as department,
        d.budget as total_budget,
        COUNT(DISTINCT e.id) as headcount,
        ROUND(AVG(EXTRACT(YEAR FROM AGE(CURRENT_DATE, e.hire_date))), 1) as avg_tenure_years,
        SUM(e.salary) as total_salary_cost,
        ROUND(SUM(e.salary) / d.budget * 100, 1) as budget_utilization
      FROM departments d
      LEFT JOIN employees e ON e.department = d.name
      GROUP BY d.name, d.budget
    ),
    project_metrics AS (
      SELECT 
        e.department,
        COUNT(DISTINCT p.id) as active_projects,
        COALESCE(SUM(pa.allocation_percent), 0) as total_project_allocation,
        COALESCE(ROUND(AVG(pa.allocation_percent), 1), 0) as avg_employee_utilization
      FROM employees e
      LEFT JOIN project_assignments pa ON e.id = pa.employee_id
      LEFT JOIN projects p ON p.id = pa.project_id AND p.status = 'active'
      GROUP BY e.department
    )
    SELECT 
      dm.*,
      pm.active_projects,
      pm.total_project_allocation,
      pm.avg_employee_utilization,
      ROUND(
        (CASE WHEN dm.budget_utilization BETWEEN 70 AND 90 THEN 25 ELSE 0 END) +
        (CASE WHEN dm.avg_tenure_years > 2 THEN 25 ELSE dm.avg_tenure_years * 12.5 END) +
        (CASE WHEN pm.avg_employee_utilization BETWEEN 70 AND 90 THEN 25 ELSE 0 END) +
        (CASE WHEN pm.active_projects > 0 THEN 25 ELSE 0 END),
        1
      ) as performance_score
    FROM department_metrics dm
    LEFT JOIN project_metrics pm ON dm.department = pm.department
    ORDER BY performance_score DESC
  `.execute(db)

  console.table(scorecard.rows)
}

async function demonstrateComplexDataTypes(db: Kysely<AdvancedSchema>) {
  console.log('\n🗃️ Complex Data Types and Operations\n')

  // 1. Array operations
  console.log('1. Working with arrays and lists:')
  
  const arrayOps = await sql<{
    name: string
    skills: string[]
    skill_count: number
    has_sql: boolean
    tech_skills: string[]
  }>`
    WITH employee_skills AS (
      SELECT 
        name,
        department,
        CASE 
          WHEN department = 'Engineering' THEN ['Python', 'SQL', 'JavaScript', 'Git', 'Docker']
          WHEN department = 'Sales' THEN ['CRM', 'Negotiation', 'Presentation', 'Excel']
          WHEN department = 'Marketing' THEN ['Analytics', 'Design', 'Content', 'Social Media']
          WHEN department = 'HR' THEN ['Recruitment', 'Excel', 'Communication', 'Policy']
          ELSE ['Excel', 'Communication']
        END as skills
      FROM employees
    )
    SELECT 
      es.name,
      es.skills,
      array_length(es.skills) as skill_count,
      array_contains(es.skills, 'SQL') as has_sql,
      array_filter(es.skills, skill -> skill IN ('Python', 'SQL', 'JavaScript', 'Git', 'Docker')) as tech_skills
    FROM employee_skills es
    WHERE es.department = 'Engineering'
  `.execute(db)

  console.table(arrayOps.rows)

  // 2. Struct and map operations
  console.log('\n2. Structured data operations:')
  
  const structOps = await sql<{
    employee_name: string
    employee_profile: any
    location_info: any
    performance_metrics: any
  }>`
    WITH employee_profiles AS (
      SELECT 
        name as employee_name,
        {
          'basic_info': {
            'name': name,
            'department': department,
            'location': location,
            'hire_date': hire_date
          },
          'compensation': {
            'salary': salary,
            'level': CASE 
              WHEN salary > 120000 THEN 'Senior'
              WHEN salary > 90000 THEN 'Mid'
              ELSE 'Junior'
            END
          }
        } as employee_profile,
        {
          'city': location,
          'timezone': CASE 
            WHEN location IN ('New York', 'Chicago') THEN 'EST'
            WHEN location = 'San Francisco' THEN 'PST'
            WHEN location = 'Los Angeles' THEN 'PST'
            WHEN location = 'Austin' THEN 'CST'
            ELSE 'Unknown'
          END,
          'remote': location = 'Remote'
        } as location_info,
        {
          'tenure_years': EXTRACT(YEAR FROM AGE(CURRENT_DATE, hire_date)),
          'salary_percentile': PERCENT_RANK() OVER (ORDER BY salary),
          'dept_rank': ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC)
        } as performance_metrics
      FROM employees
    )
    SELECT * FROM employee_profiles
    WHERE employee_profile.compensation.salary > 100000
    LIMIT 5
  `.execute(db)

  console.table(structOps.rows)
}

async function main() {
  const database = await DuckDBInstance.create(':memory:')
  
  const db = new Kysely<AdvancedSchema>({
    dialect: new DuckDbDialect({
      database,
      config: {
        threads: 4,
        max_memory: '1GB'
      }
    }),
  })

  try {
    console.log('⚡ Advanced Queries and Analytics Demo\n')

    // Create comprehensive sample data
    await createSampleData(db)

    // Demonstrate different advanced query patterns
    await demonstrateCTEs(db)
    await demonstrateWindowFunctions(db)
    await demonstrateRecursiveQueries(db)
    await demonstrateAdvancedJoins(db)
    await demonstrateAnalyticalFunctions(db)
    await demonstrateComplexDataTypes(db)

    console.log('\n✅ Advanced queries demo complete!')

  } finally {
    await db.destroy()
    database.closeSync()
  }
}

// Run the example
if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch(console.error)
}

export { main }