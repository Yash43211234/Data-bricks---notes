# Databricks Lakeflow Jobs --- Detailed Notes

> **Source:** Notes prepared from the provided Lakeflow Jobs masterclass
> transcript.\
> **Scope:** Fundamentals → tasks → dependencies → conditions → loops →
> dynamic values → SQL outputs → real-world dynamic ingestion → mapping
> tables → alerts → schedules/triggers → parameters → compute →
> notifications.

------------------------------------------------------------------------

## 1. What is Lakeflow Jobs?

### Core idea

**Lakeflow Jobs is Databricks' workflow/orchestration framework.**

Its main purpose is not primarily to transform data. Its purpose is to
**orchestrate work**:

-   decide **what** should run
-   decide **when** it should run
-   decide **what must finish before what**
-   run tasks **sequentially or in parallel**
-   apply **conditions**
-   execute **loops**
-   pass **dynamic values** between tasks
-   retry failed tasks
-   monitor runs
-   send notifications
-   schedule jobs
-   trigger jobs from events such as file arrival

A useful mental model:

``` text
Lakeflow Job
     |
     +-- Task A
     |     |
     |     +-- Task B
     |     +-- Task C
     |
     +-- Conditions
     +-- Loops
     +-- Parameters
     +-- Notifications
     +-- Schedule / Triggers
```

### Important distinction

A Lakeflow Job is essentially the **orchestration layer**.

A notebook, SQL query, pipeline, Python script, etc. performs the actual
work.

------------------------------------------------------------------------

# 2. Orchestration

## 2.1 What does orchestration mean?

Orchestration means controlling the **flow and order of multiple
activities**.

Example:

``` text
Read data
   ↓
Clean data
   ↓
Transform data
   ↓
Load Silver
   ↓
Build Gold
   ↓
Refresh dashboard
```

The orchestrator decides:

-   when the first step starts
-   when the next step can start
-   what happens if a step fails
-   which steps can run together
-   whether a step should run only under a condition
-   whether work should repeat

------------------------------------------------------------------------

# 3. DAG --- Directed Acyclic Graph

A workflow can be represented as a **DAG**.

### DAG means

**D** --- Directed\
**A** --- Acyclic\
**G** --- Graph

### Directed

There is a defined direction:

``` text
A → B → C
```

This means:

``` text
A must lead to B
B must lead to C
```

### Acyclic

The workflow does not normally create a circular dependency such as:

``` text
A → B → C
↑       |
└───────┘
```

That would create a cycle / potentially an infinite loop.

### Graph

Tasks are represented as nodes connected by dependencies.

Example:

``` text
        A
       / \
      ↓   ↓
      B   C
```

This means:

1.  A runs first.
2.  B and C can run after A.
3.  B and C can run in parallel if their dependencies allow it.

------------------------------------------------------------------------

# 4. Jobs vs Pipelines in Databricks

This is one of the most important concepts.

## 4.1 Pipelines

A pipeline is used to **process/transform data**.

For example:

``` text
Raw Data
   ↓
Bronze
   ↓
Silver
   ↓
Gold
```

The pipeline contains data-processing logic.

------------------------------------------------------------------------

## 4.2 Jobs / Workflows

A Job is used to **orchestrate work**.

For example:

``` text
Bronze Pipeline
      ↓
Silver Pipeline
      ↓
Gold Pipeline
      ↓
Dashboard Refresh
```

The job controls when each component runs.

------------------------------------------------------------------------

## 4.3 A Job can orchestrate pipelines

For example:

``` text
Lakeflow Job
     |
     +-- Bronze Pipeline
     |
     +-- Silver Pipeline
     |
     +-- Gold Pipeline
```

The Job can also orchestrate things other than pipelines:

-   notebooks
-   SQL queries
-   SQL files
-   Python scripts
-   Python wheels
-   JARs
-   dbt jobs
-   dashboards
-   alerts
-   other jobs
-   etc.

### Key takeaway

``` text
Pipeline = performs data processing

Job = orchestrates work
```

A Job can therefore be thought of as the **workflow/controller layer
around different pieces of work**.

------------------------------------------------------------------------

# 5. Why use Databricks Jobs?

The transcript explains that many organizations historically use
external orchestration tools with Databricks.

Examples mentioned:

-   Azure Data Factory (ADF)
-   Azure Synapse-related orchestration
-   Apache Airflow
-   other orchestration tools

The idea behind Lakeflow Jobs is to provide orchestration **inside the
Databricks platform**, reducing dependency on separate orchestration
systems for suitable workloads.

> **Important:** The transcript describes this as an industry trend and
> expected direction. Treat statements about future demand/adoption as
> the instructor's framing rather than as independently verified market
> statistics.

------------------------------------------------------------------------

# 6. Tasks

## 6.1 What is a Task?

A **Task is an individual unit of work inside a Job.**

Examples:

``` text
Task A → Notebook A
Task B → Notebook B
Task C → SQL Query
```

A simple job may contain only one task:

``` text
Job
 |
 └── Task A
       |
       └── Notebook A
```

------------------------------------------------------------------------

# 7. Sequential Tasks

Suppose there are two notebooks:

``` text
Notebook A
Notebook B
```

Requirement:

> Notebook B should run only after Notebook A.

Create:

``` text
Task A → Notebook A
     ↓
Task B → Notebook B
```

This creates a dependency:

``` text
Task B depends on Task A
```

------------------------------------------------------------------------

# 8. Parallel Tasks

Suppose:

``` text
Notebook A
Notebook B
Notebook C
```

Requirement:

1.  A must run first.
2.  After A finishes, B and C should run together.

Workflow:

``` text
          A
         / \
        ↓   ↓
        B   C
```

This is more efficient than:

``` text
A → B → C
```

when B and C are independent.

### Important interview point

**Parallelism is possible when tasks do not depend on each other.**

------------------------------------------------------------------------

# 9. Task Dependency

Each task can specify which task it depends on.

Example:

``` text
Task A
   ↓
Task B
```

Task B:

``` text
Depends on: Task A
```

The dependency controls when Task B becomes eligible to run.

------------------------------------------------------------------------

# 10. Run-if Conditions

A dependent task does not always have to run only when the previous task
succeeds.

The transcript discusses different dependency outcomes.

For example:

### Run after success

``` text
A succeeds
   ↓
B runs
```

### Run after failure

A task can be configured to run when an upstream task fails.

Conceptually:

``` text
A fails
  ↓
Failure-handling task runs
```

This can be useful for:

-   error handling
-   cleanup
-   notifications
-   recovery logic

------------------------------------------------------------------------

# 11. Retry Policy

Real-world pipelines can fail because of temporary/intermittent issues.

Examples:

-   temporary network problem
-   transient service issue
-   temporary resource problem

A retry policy allows the task to try again.

Example configuration:

``` text
Number of retries = 1
Wait before retry = 15 seconds
```

Flow:

``` text
Task A
  ↓
Fails
  ↓
Wait 15 sec
  ↓
Retry
```

If the retry succeeds, the workflow can continue according to the
dependency configuration.

### Why retries matter

A temporary failure should not necessarily make the entire workflow
permanently fail.

------------------------------------------------------------------------

# 12. If/Else Conditions

Lakeflow Jobs can use conditional logic.

Example requirement:

``` text
Task A
   ↓
Check condition
   |
   +-- TRUE  → Task B
   |
   +-- FALSE → Task C
```

This is different from simple parallel execution.

------------------------------------------------------------------------

## 12.1 Example: weekday/weekend

Suppose:

-   Task B should run on a particular weekend day.
-   Task C should run otherwise.

Conceptually:

``` text
Task A
   ↓
IF condition
   |
   +-- TRUE  → Task B
   |
   +-- FALSE → Task C
```

The transcript demonstrates using dynamic date/time values such as:

``` text
job.start_time
```

and weekday-related values/functions.

------------------------------------------------------------------------

# 13. Dynamic Values

Dynamic values allow a workflow to use information that is available
**at runtime** rather than hard-coding values.

Example:

``` text
Task X
  |
  | produces:
  | total_records = 3
  ↓
Task Y
  |
  | receives:
  | records_processed = 3
```

Instead of writing:

``` text
records_processed = 3
```

you dynamically pass the value from Task X.

This is a major concept for real-world workflows.

------------------------------------------------------------------------

# 14. Task Values

The transcript demonstrates the concept of **task values**.

Suppose Notebook X calculates:

``` python
total_records = df.count()
```

The value might be:

``` text
3
```

The task can publish that value so another task can consume it.

Conceptually:

``` text
Notebook X
    |
    | total_records = 3
    ↓
Notebook Y
```

------------------------------------------------------------------------

## 14.1 Setting a task value

The transcript uses the Databricks utility pattern:

``` python
dbutils.jobs.taskValues.set(
    key="total_records",
    value=total_records
)
```

### Key

``` text
total_records
```

acts as the identifier for the value.

### Value

``` text
total_records
```

is the actual runtime value.

Conceptually:

``` text
key → value

total_records → 3
```

------------------------------------------------------------------------

# 15. Notebook Parameters

A notebook can define a parameter using widgets.

The transcript demonstrates:

``` python
dbutils.widgets.text("records_processed", "")
```

Then retrieve it using:

``` python
dbutils.widgets.get("records_processed")
```

This makes notebooks more reusable.

Instead of hard-coding:

``` python
records_processed = 10
```

the notebook can receive the value from the Job.

------------------------------------------------------------------------

# 16. Why Parameters Are Useful

Without parameters:

``` text
Notebook
   |
   └── hard-coded input
```

With parameters:

``` text
Job
 |
 ├── runtime value
 |
 ↓
Notebook parameter
```

Benefits:

-   reusable notebooks
-   less hard-coding
-   easier orchestration
-   easier testing
-   dynamic workflows
-   environment-specific values

------------------------------------------------------------------------

# 17. Dynamic Value Reference

The transcript introduces a dynamic reference pattern conceptually like:

``` text
{{tasks.<task_name>.values.<key>}}
```

For example:

``` text
{{tasks.task_x.values.total_records}}
```

Meaning:

> Get `total_records` from `task_x`.

Then the value can be passed into a downstream task parameter.

Example:

``` text
Task X
  |
  | total_records = 3
  ↓
Task Y
  |
  | records_processed = {{tasks.task_x.values.total_records}}
```

Task Y receives:

``` text
3
```

without hard-coding it.

------------------------------------------------------------------------

# 18. Task Values vs Notebook Parameters

These are related but different.

### Task Value

Used to **publish/output information from one task**.

``` text
Task X
  ↓
publishes total_records
```

### Notebook Parameter

Used to **receive input inside another notebook**.

``` text
Task Y
  ↓
receives records_processed
```

Together:

``` text
Task X
  |
  | Task Value
  ↓
Job dynamic reference
  |
  | Parameter
  ↓
Task Y
```

------------------------------------------------------------------------

# 19. Directly Reading a Task Value in Another Notebook

The transcript also demonstrates accessing a task value directly in
notebook code.

Conceptually:

``` python
dbutils.jobs.taskValues.get(
    taskKey="task_x",
    key="total_records"
)
```

This allows the downstream notebook to retrieve the value directly.

------------------------------------------------------------------------

# 20. Multiple Task Values

A single task can publish multiple values.

Example:

``` text
Task X
 |
 +-- total_records
 |
 +-- order_id
 |
 +-- file_name
```

So there is no requirement that a task publish only one value.

Example:

``` python
dbutils.jobs.taskValues.set(
    key="total_records",
    value=total_records
)

dbutils.jobs.taskValues.set(
    key="order_id",
    value=order_id
)
```

------------------------------------------------------------------------

# 21. Repair Runs

A very useful operational feature demonstrated in the transcript is
**repairing failed tasks**.

Suppose:

``` text
Task X → SUCCESS
Task Y → SUCCESS
Task Z → FAILED
```

You do not necessarily want to execute everything again.

Instead, a repair can rerun the failed portion.

Conceptually:

``` text
X ✓
Y ✓
Z ✗
   ↓
Repair
   ↓
Z runs again
```

### Why this matters

For large workflows, rerunning every successful upstream task can:

-   waste compute
-   take additional time
-   repeat unnecessary work

Repairing from the failed portion is therefore useful operationally.

------------------------------------------------------------------------

# 22. Loops / For Each

Lakeflow Jobs supports looping over a collection.

The transcript introduces a **For Each** pattern.

Example input:

``` text
[1, 2, 3, 4, 5]
```

A task can be executed once for each item.

Conceptually:

``` text
1 → Task
2 → Task
3 → Task
4 → Task
5 → Task
```

------------------------------------------------------------------------

# 23. Iterable Data

For a loop, you need something that can be iterated.

Examples mentioned:

-   arrays/lists
-   tuples
-   strings
-   other iterable structures

A common pattern is an array/list:

``` python
[1, 2, 3, 4, 5]
```

The loop processes each item.

------------------------------------------------------------------------

# 24. Loop Iterations

If the input is:

``` text
[1, 2, 3, 4, 5]
```

there are five iterations.

The transcript notes that the UI represents iterations using zero-based
indexing:

``` text
0
1
2
3
4
```

for five elements.

------------------------------------------------------------------------

# 25. Loop Concurrency

By default, iterations can execute one at a time.

Conceptually:

``` text
1
↓
2
↓
3
↓
4
↓
5
```

With concurrency, multiple iterations can execute at the same time.

Example:

``` text
Concurrency = 5

1 ─┐
2 ─┤
3 ─┼── run together
4 ─┤
5 ─┘
```

### Important distinction

**Looping** answers:

> How many times should the task execute?

**Concurrency** answers:

> How many iterations can execute at the same time?

------------------------------------------------------------------------

# 26. Concurrency and Resource Usage

The transcript recommends thinking about concurrency carefully.

Do not automatically set concurrency equal to the entire array size in
every real-world case.

Higher concurrency can increase:

-   parallel workload
-   compute usage
-   resource pressure

The appropriate value depends on:

-   workload
-   compute capacity
-   cost
-   execution time
-   dependencies

------------------------------------------------------------------------

# 27. Real-World Dynamic File Ingestion

This is one of the most important examples in the transcript.

Suppose a source contains:

``` text
file1
file2
file3
```

You want to process all files using **one reusable ingestion notebook**.

Instead of creating:

``` text
Notebook A → file1
Notebook B → file2
Notebook C → file3
```

you create:

``` text
One ingestion notebook
        ↑
        |
For Each file
```

------------------------------------------------------------------------

# 28. Parameterized Ingestion Notebook

The ingestion notebook receives:

``` text
file_name
```

using a widget/parameter.

Conceptually:

``` python
dbutils.widgets.text("file_name", "")
file_name = dbutils.widgets.get("file_name")
```

Then the notebook constructs the input path dynamically.

Example idea:

``` text
volume/path/{file_name}
```

The same notebook can therefore process:

``` text
file1
file2
file3
```

------------------------------------------------------------------------

# 29. Passing Loop Input to a Task

Inside a For Each loop, the current iteration's input can be referenced
through the loop input.

Conceptually:

``` text
input.file_name
```

If the current item is:

``` text
file1
```

then:

``` text
input.file_name
```

resolves to:

``` text
file1
```

Next iteration:

``` text
input.file_name = file2
```

and so on.

------------------------------------------------------------------------

# 30. Dynamic File Processing Architecture

The demonstrated design can be visualized as:

``` text
Source
 |
 +-- file1
 +-- file2
 +-- file3
 |
 ↓
Task produces file-name array
 |
 ↓
For Each
 |
 +-- input.file_name = file1
 |       ↓
 |   Ingestion Notebook
 |
 +-- input.file_name = file2
 |       ↓
 |   Ingestion Notebook
 |
 +-- input.file_name = file3
         ↓
     Ingestion Notebook
```

This is much more reusable than hard-coding individual files.

------------------------------------------------------------------------

# 31. Mapping Tables

The transcript then moves from a notebook-generated array toward a
**mapping table**.

### Why?

Suppose you have a large list of files/configuration values.

Keeping the mapping data inside notebook code can become difficult to
maintain.

Instead, store the mapping in a table.

Example:

``` text
mapping table

file_name
---------
orders
products
regions
```

Then the workflow reads the table.

------------------------------------------------------------------------

# 32. Mapping Table Architecture

Instead of:

``` text
Notebook
   |
   └── hard-coded array
```

use:

``` text
Mapping Table
     |
     ↓
SQL Query
     |
     ↓
Array / SQL Output
     |
     ↓
For Each
     |
     ↓
Parameterized Ingestion Task
```

This separates:

``` text
Workflow logic
```

from:

``` text
Mapping/configuration data
```

------------------------------------------------------------------------

# 33. SQL Query Output

The transcript demonstrates using a SQL query as the source for the For
Each input.

A query can return multiple rows.

Conceptually:

``` text
SELECT *
FROM mapping;
```

Output:

``` text
row 1
row 2
row 3
```

The Job can use the SQL output as the input collection for the loop.

------------------------------------------------------------------------

# 34. SQL Output as List of Dictionaries

The transcript explains that multiple SQL rows can be represented
conceptually as an array/list of dictionaries.

Example:

``` python
[
    {
        "id": 1,
        "customer_id": 101,
        "status": "complete"
    },
    {
        "id": 2,
        "customer_id": 102,
        "status": "pending"
    }
]
```

Each row becomes a dictionary-like object containing column/value pairs.

------------------------------------------------------------------------

# 35. SQL Output References

The transcript discusses references such as:

``` text
output.rows
```

to access the rows returned by a SQL task.

It also mentions:

``` text
output.first_row
```

when only the first row is required.

### Mental model

``` text
SQL Task
   |
   +-- output.rows
   |
   +-- output.first_row
```

------------------------------------------------------------------------

# 36. SQL Parameters

SQL tasks can also use parameters.

The transcript demonstrates parameterized SQL instead of hard-coding a
value.

Conceptually:

``` sql
SELECT *
FROM orders
WHERE id = :id;
```

The value for `id` can then be supplied dynamically by the Job.

This is useful when the SQL task should process different values on
different runs.

------------------------------------------------------------------------

# 37. Dynamic SQL Example

Suppose the source task produces:

``` text
order_id = 3
```

Instead of:

``` sql
WHERE id = 3
```

you use a parameter:

``` sql
WHERE id = :id
```

and let the Job provide:

``` text
id = 3
```

This makes the SQL reusable.

------------------------------------------------------------------------

# 38. Notebook → SQL Dynamic Flow

A useful architecture from the transcript is:

``` text
Notebook X
   |
   | order_id = 3
   ↓
Task Value
   |
   ↓
SQL Task parameter
   |
   ↓
Parameterized SQL
```

This allows values generated by one task to control another task.

------------------------------------------------------------------------

# 39. Job Parameters

The transcript also introduces **Job-level parameters**.

There are two levels to understand:

### Task/activity-level parameters

Parameters configured for a specific task.

``` text
Task A
  └── parameter
```

### Job-level parameters

Parameters defined at the parent Job level and potentially reused by
multiple tasks.

``` text
Job
 |
 +-- parameter X
 |
 +-- Task A
 |
 +-- Task B
 |
 +-- Task C
```

This can be useful when several tasks need the same input/configuration.

------------------------------------------------------------------------

# 40. When to Use Job-Level Parameters

Job-level parameters can be useful for shared values such as:

``` text
environment = dev
date = 2026-09-25
source = sales
region = APAC
```

Then multiple tasks can use them.

The transcript emphasizes that this is a design choice, not something
that must always be used.

------------------------------------------------------------------------

# 41. Supported Task Types

The transcript demonstrates or mentions multiple types of work that can
be added as tasks.

Examples include:

-   Notebook
-   SQL Query
-   SQL File
-   Python Script
-   Python Wheel
-   JAR
-   ETL / declarative pipeline
-   dbt Job
-   another Job
-   SQL Alert
-   Dashboard refresh
-   other Databricks-related activities

The key point is:

> A Lakeflow Job is not limited to notebooks.

------------------------------------------------------------------------

# 42. Jobs Can Orchestrate Other Jobs

The transcript specifically points out that a Job can invoke another
Job.

Conceptually:

``` text
Parent Job
    |
    ↓
Child Job
```

This allows larger workflows to be composed from smaller workflows.

------------------------------------------------------------------------

# 43. Jobs Can Orchestrate Pipelines

Similarly:

``` text
Job
 |
 +-- Pipeline A
 |
 +-- Pipeline B
 |
 +-- Notebook
 |
 +-- SQL
```

This makes the Job a central orchestration layer.

------------------------------------------------------------------------

# 44. Alerts

The transcript covers Databricks alerts separately from task/job
notifications.

An alert can be based on a SQL result.

Example business condition:

``` text
Total Revenue >= 300
```

If the condition becomes true, a notification can be sent.

------------------------------------------------------------------------

# 45. Example Alert Flow

Suppose:

``` sql
SELECT SUM(total_amount) AS total_revenue
FROM orders;
```

Result:

``` text
total_revenue
-------------
350
```

Alert condition:

``` text
total_revenue >= 300
```

Since:

``` text
350 >= 300
```

the alert condition is satisfied.

The transcript describes configuring a notification destination for the
alert.

------------------------------------------------------------------------

# 46. Modern vs Legacy Alerts

The transcript recommends using the modern alert interface rather than
the legacy alert option, noting that legacy functionality was expected
to be deprecated.

For practical work, use the currently supported Databricks alert
mechanism available in your workspace.

------------------------------------------------------------------------

# 47. Job Scheduling

Manually clicking **Run Now** every day is not a production workflow.

Once a Job is ready, it can be scheduled.

Example:

``` text
Every day
at 09:30 AM
```

Conceptually:

``` text
Schedule
   ↓
Job starts
   ↓
Tasks execute
```

------------------------------------------------------------------------

# 48. Trigger Types Mentioned

The transcript discusses:

1.  Scheduled trigger
2.  File arrival trigger
3.  Continuous execution mode

------------------------------------------------------------------------

## 48.1 Scheduled Trigger

Example:

``` text
Every day at 9:30 AM
```

Useful for:

-   daily ETL
-   daily reporting
-   daily data processing
-   batch workflows

------------------------------------------------------------------------

## 48.2 File Arrival Trigger

Conceptually:

``` text
New file arrives
      ↓
Trigger Job
      ↓
Process file
```

This is similar in concept to event-driven storage/file-arrival
triggering.

The transcript compares it conceptually with storage-event triggering in
Azure Data Factory.

------------------------------------------------------------------------

## 48.3 Continuous Mode

The transcript describes continuous execution as keeping an active run
and creating another run when appropriate.

Conceptually:

``` text
Run 1 finishes
      ↓
Run 2 starts
      ↓
Run 2 finishes
      ↓
Run 3 starts
```

The transcript compares the idea to continuous/streaming-style execution
concepts.

------------------------------------------------------------------------

# 49. File Arrival vs Scheduled Trigger

### Scheduled

``` text
Time-based
```

Example:

``` text
Every day at 9 AM
```

### File Arrival

``` text
Event-based
```

Example:

``` text
File arrives → start Job
```

This distinction is important when designing pipelines.

------------------------------------------------------------------------

# 50. Compute

The transcript also discusses compute configuration.

### Job Compute

Compute associated with a Job/task execution.

The transcript explains the idea that job compute can be started for the
workload and stopped automatically after the Job finishes, depending on
the configured compute model.

### Serverless

The examples use Serverless compute in the Databricks Free Edition
environment.

------------------------------------------------------------------------

# 51. Performance Optimization

The transcript repeatedly demonstrates a **performance optimized**
setting for the Job.

The instructor notes that without appropriate optimization, runs in the
Free Edition environment can take longer.

### Important practical point

Free Edition has limited resources, so:

-   avoid unnecessary Jobs
-   avoid leaving unnecessary workloads running
-   use resources carefully
-   expect temporary resource exhaustion in some situations

------------------------------------------------------------------------

# 52. Free Edition Resource Considerations

The transcript mentions that users may encounter temporary
resource/compute exhaustion in the Free Edition.

The instructor's workaround is to avoid maintaining many unnecessary
Jobs and wait for resources to become available if temporary exhaustion
occurs.

> This is environment-specific behavior described in the provided
> transcript, not a universal statement about all Databricks workspaces.

------------------------------------------------------------------------

# 53. Notifications

Notifications can be configured for a Job.

Possible events discussed include:

-   success
-   failure
-   duration-related conditions

Example:

``` text
Job fails
   ↓
Email notification
```

or:

``` text
Job takes longer than expected
   ↓
Notification
```

------------------------------------------------------------------------

# 54. Task-Level Notifications

Notifications can also be configured at the task level.

This is useful when you care about a particular task rather than the
entire Job.

Example:

``` text
Task A fails
   ↓
Send notification
```

------------------------------------------------------------------------

# 55. Job-Level vs Task-Level Notifications

### Task level

Useful for a specific activity:

``` text
Task X → alert if failed
```

### Job level

Useful for the overall workflow:

``` text
Job → alert if failed
```

Choose based on the monitoring requirement.

------------------------------------------------------------------------

# 56. Notification Destinations

The transcript describes configuring a notification destination through
Databricks settings and then selecting that destination for
alerts/notifications.

Conceptually:

``` text
Settings
   ↓
Notification Destination
   ↓
Email / supported destination
   ↓
Job / Alert
```

------------------------------------------------------------------------

# 57. Notebook vs SQL vs SQL File

The transcript distinguishes different task forms.

### Notebook

Useful when logic is implemented in:

``` text
Python
PySpark
SQL
```

inside a notebook.

### SQL Query

Useful when the SQL is managed as a query/task.

### SQL File

Useful when SQL is stored as a file and executed as a Job task.

The transcript's author prefers SQL files in some development scenarios,
while also using SQL queries for specific demonstrations.

------------------------------------------------------------------------

# 58. Volumes in the Example

For the dynamic ingestion example, the transcript uses a Databricks
Volume as a place to store sample raw files.

Conceptually:

``` text
Catalog
  ↓
Schema
  ↓
Volume
  ↓
Raw files
```

Example path pattern:

``` text
/Volumes/<catalog>/<schema>/<volume>/...
```

The exact workspace setup can vary.

------------------------------------------------------------------------

# 59. Dynamic Ingestion --- Complete Architecture

The advanced example combines many concepts.

``` text
                 Mapping Source
                      |
                      ↓
              File-name / mapping list
                      |
                      ↓
                 For Each
                      |
          +-----------+-----------+
          |           |           |
          ↓           ↓           ↓
       File 1      File 2      File 3
          |           |           |
          +-----------+-----------+
                      |
                      ↓
          Parameterized Ingestion Task
                      |
                      ↓
                 Read File
                      |
                      ↓
                 Process Data
```

The same notebook is reused for every iteration.

------------------------------------------------------------------------

# 60. Why This Pattern Is Powerful

Instead of building:

``` text
Task 1 → file1
Task 2 → file2
Task 3 → file3
...
Task 100 → file100
```

you can build:

``` text
For Each
   ↓
One reusable task
```

and feed it the current file.

This reduces duplication and makes the workflow easier to extend.

------------------------------------------------------------------------

# 61. Mapping Table --- More Maintainable Configuration

Suppose there are 100 or 1,000 files.

Hard-coding the list in a notebook becomes less convenient.

A mapping table can store:

``` text
file_name
---------
orders
customers
products
regions
...
```

Then the Job reads the table.

Benefits:

-   centralized mapping
-   easier updates
-   SQL-based management
-   easier maintenance
-   separates configuration from processing logic

------------------------------------------------------------------------

# 62. Complete End-to-End Example

A production-style conceptual flow from the transcript is:

``` text
Mapping Table
      |
      ↓
SQL Query
      |
      ↓
SQL Output Rows
      |
      ↓
For Each
      |
      ↓
Current Input
      |
      ↓
Ingestion Notebook
      |
      ↓
Read current file
      |
      ↓
Transform / Load
```

With concurrency:

``` text
Mapping Table
      |
      ↓
SQL Query
      |
      ↓
Rows
      |
      ↓
For Each
   /   |   \
  ↓    ↓    ↓
File1 File2 File3
  |    |    |
  +----+----+
       |
   parallel execution
```

------------------------------------------------------------------------

# 63. Core Mental Model

Remember Lakeflow Jobs using this structure:

``` text
JOB
│
├── TRIGGER
│   ├── Schedule
│   ├── File Arrival
│   └── Continuous
│
├── TASKS
│   ├── Notebook
│   ├── SQL
│   ├── Python
│   ├── Pipeline
│   ├── dbt
│   ├── Dashboard
│   └── Other task types
│
├── DEPENDENCIES
│   ├── Sequential
│   └── Parallel
│
├── CONTROL FLOW
│   ├── If/Else
│   └── For Each
│
├── DATA PASSING
│   ├── Parameters
│   ├── Task Values
│   ├── Dynamic References
│   └── SQL Outputs
│
├── RELIABILITY
│   ├── Retry
│   └── Repair Runs
│
├── MONITORING
│   ├── Alerts
│   └── Notifications
│
└── COMPUTE
    └── Serverless / Job Compute
```

------------------------------------------------------------------------

# 64. Most Important Differences to Memorize

  -----------------------------------------------------------------------
  Concept                             Meaning
  ----------------------------------- -----------------------------------
  Job                                 Orchestrates work

  Task                                One unit of work inside a Job

  Pipeline                            Performs data processing

  DAG                                 Directed dependency graph

  Dependency                          Defines execution relationship

  Parallel task                       Runs when its dependencies allow
                                      and can execute alongside
                                      independent tasks

  If/Else                             Chooses execution path based on a
                                      condition

  For Each                            Repeats a task for each input item

  Concurrency                         Number of loop iterations allowed
                                      to run together

  Parameter                           Input provided to a task/notebook

  Task Value                          Runtime value published by a task

  Dynamic Reference                   Retrieves a runtime value from
                                      another part of the workflow

  SQL Output                          Data returned by a SQL task that
                                      can be consumed downstream

  Retry                               Reattempts a failed task

  Repair                              Reruns failed portion without
                                      unnecessarily rerunning successful
                                      work

  Schedule                            Starts a Job at defined times

  File Arrival                        Starts a Job when configured
                                      data/file arrival occurs

  Alert                               Evaluates a condition/query and can
                                      notify

  Notification                        Communicates Job/task events
  -----------------------------------------------------------------------

------------------------------------------------------------------------

# 65. Interview-Oriented Questions

## Q1. What is a Databricks Job?

A Databricks Job/Lakeflow Job is used to orchestrate tasks and workflows
in Databricks.

------------------------------------------------------------------------

## Q2. What is the difference between a Job and a Pipeline?

A pipeline is focused on data processing/ETL, while a Job is the
orchestration layer that can coordinate pipelines and other tasks.

------------------------------------------------------------------------

## Q3. What is a Task?

A Task is an individual unit of work inside a Job, such as a notebook,
SQL query, Python script, pipeline, or other supported activity.

------------------------------------------------------------------------

## Q4. How do you run two tasks in parallel?

Give both tasks the same upstream dependency rather than making one
dependent on the other.

Example:

``` text
       A
      / \
     B   C
```

------------------------------------------------------------------------

## Q5. What is a DAG?

A Directed Acyclic Graph represents tasks and their directional
dependencies without circular dependency paths.

------------------------------------------------------------------------

## Q6. Why use task values?

To pass runtime-generated information from one task to another.

Example:

``` text
Task A → total_records → Task B
```

------------------------------------------------------------------------

## Q7. What is the difference between a parameter and a task value?

A parameter is generally an input to a task; a task value is runtime
information published by an upstream task.

------------------------------------------------------------------------

## Q8. Why use a For Each loop?

To execute the same task for multiple input items, such as multiple
files.

------------------------------------------------------------------------

## Q9. What is concurrency in a For Each loop?

It controls how many loop iterations can execute concurrently.

------------------------------------------------------------------------

## Q10. What happens if a task fails temporarily?

A retry policy can be configured so the task is attempted again after a
specified delay.

------------------------------------------------------------------------

## Q11. What is a repair run?

It allows failed parts of a workflow to be rerun without unnecessarily
rerunning already successful portions.

------------------------------------------------------------------------

## Q12. How can a Job start automatically?

Using triggers such as:

-   schedule
-   file arrival
-   continuous execution mode

------------------------------------------------------------------------

## Q13. How do you dynamically pass a value to another task?

A common pattern is:

``` text
Task A
  ↓
Task Value
  ↓
Dynamic Reference
  ↓
Task B Parameter
```

------------------------------------------------------------------------

## Q14. Why use a mapping table?

To store mapping/configuration information outside the processing
notebook, making it easier to maintain and update.

------------------------------------------------------------------------

## Q15. Why use one ingestion notebook inside a loop?

Because the same processing logic can be reused for many files instead
of creating separate notebooks/tasks for every file.

------------------------------------------------------------------------

# 66. Common Mistakes to Avoid

### Mistake 1: Confusing Job and Pipeline

Remember:

``` text
Pipeline → processing
Job → orchestration
```

------------------------------------------------------------------------

### Mistake 2: Making independent tasks sequential

Bad when unnecessary:

``` text
A → B → C
```

Better when B and C are independent:

``` text
    A
   / \
  B   C
```

------------------------------------------------------------------------

### Mistake 3: Hard-coding runtime values

Instead of:

``` text
records = 100
```

use:

``` text
Task Value → Dynamic Reference → Parameter
```

when the value is generated at runtime.

------------------------------------------------------------------------

### Mistake 4: Creating one task per file

Instead of:

``` text
Task 1 → file1
Task 2 → file2
Task 3 → file3
```

consider:

``` text
For Each
   ↓
One reusable ingestion task
```

------------------------------------------------------------------------

### Mistake 5: Ignoring retry configuration

Transient failures can happen. Configure retries where appropriate.

------------------------------------------------------------------------

### Mistake 6: Rerunning the entire workflow after one failure

If only one downstream task failed, consider a repair run rather than
rerunning successful upstream work unnecessarily.

------------------------------------------------------------------------

### Mistake 7: Using notebook code as a large configuration table

For maintainable mappings, a table can be a better place to store
configuration/mapping data.

------------------------------------------------------------------------

# 67. Practical Learning Sequence

The transcript's concepts can be learned in this order:

``` text
1. Job
   ↓
2. Task
   ↓
3. Dependencies
   ↓
4. Parallel Tasks
   ↓
5. If/Else
   ↓
6. For Each
   ↓
7. Concurrency
   ↓
8. Parameters
   ↓
9. Task Values
   ↓
10. Dynamic References
   ↓
11. SQL Outputs
   ↓
12. Dynamic SQL
   ↓
13. Dynamic File Ingestion
   ↓
14. Mapping Tables
   ↓
15. Alerts
   ↓
16. Scheduling / Triggers
   ↓
17. Notifications
   ↓
18. Repair / Operational Handling
```

------------------------------------------------------------------------

# 68. One Big Example to Remember

Imagine a daily data platform:

``` text
                    SCHEDULE
                       |
                       ↓
                  LAKEFLOW JOB
                       |
                       ↓
                 Load Mapping
                       |
                       ↓
                  SQL Query
                       |
                       ↓
                  For Each
                /     |      \
               /      |       \
              ↓       ↓        ↓
           File A   File B   File C
              |       |        |
              +-------+--------+
                      |
                      ↓
             Ingestion Notebook
                      |
                      ↓
                 Bronze Data
                      |
                      ↓
              Transformation
                      |
                      ↓
                  Silver/Gold
                      |
                      ↓
                Dashboard
                      |
                      ↓
                    Alert
```

This single diagram combines:

-   Job
-   trigger
-   tasks
-   SQL
-   dynamic output
-   For Each
-   concurrency
-   parameters
-   ingestion
-   data processing
-   downstream orchestration
-   notifications/alerts

------------------------------------------------------------------------

# 69. Final Cheat Sheet

``` text
Lakeflow Jobs
│
├── Purpose
│   └── Orchestration
│
├── Job
│   └── Collection of tasks + workflow rules
│
├── Task
│   └── Individual executable activity
│
├── Dependency
│   └── Controls task order
│
├── Parallelism
│   └── Independent tasks can run together
│
├── If/Else
│   └── Conditional execution
│
├── For Each
│   └── Repeat task for each input
│
├── Concurrency
│   └── Number of loop iterations running together
│
├── Parameters
│   └── Inputs to tasks
│
├── Task Values
│   └── Runtime values produced by tasks
│
├── Dynamic References
│   └── Read runtime values from other tasks
│
├── SQL Output
│   └── Pass query results downstream
│
├── Retry
│   └── Retry failed tasks
│
├── Repair
│   └── Rerun failed portion
│
├── Alerts
│   └── Condition-based notification
│
├── Schedule
│   └── Time-based trigger
│
├── File Arrival
│   └── Event-based trigger
│
└── Notifications
    └── Success / failure / duration events
```

------------------------------------------------------------------------

# 70. What You Should Practice Yourself

Do not only read these notes. Rebuild the examples in Databricks Free
Edition.

### Practice 1 --- Basic Job

Create:

``` text
Task A
```

Run one notebook.

------------------------------------------------------------------------

### Practice 2 --- Sequential

Create:

``` text
A → B
```

------------------------------------------------------------------------

### Practice 3 --- Parallel

Create:

``` text
    A
   / \
  B   C
```

------------------------------------------------------------------------

### Practice 4 --- If/Else

Create:

``` text
       A
       ↓
   condition
    /     \
 TRUE    FALSE
  ↓        ↓
  B        C
```

------------------------------------------------------------------------

### Practice 5 --- For Each

Input:

``` text
[1, 2, 3, 4, 5]
```

Run the same notebook for every item.

------------------------------------------------------------------------

### Practice 6 --- Concurrency

Run the same For Each workflow with:

``` text
Concurrency = 1
```

and then with a higher value.

Observe the difference.

------------------------------------------------------------------------

### Practice 7 --- Task Values

Task X:

``` text
total_records = df.count()
```

Publish it as a task value.

Task Y:

``` text
receive total_records
```

Print the value.

------------------------------------------------------------------------

### Practice 8 --- Dynamic SQL

Create:

``` text
Task X → order_id
             ↓
         SQL Task
             ↓
       WHERE id = :id
```

------------------------------------------------------------------------

### Practice 9 --- Dynamic File Ingestion

Create:

``` text
[file1, file2, file3]
        ↓
     For Each
        ↓
One ingestion notebook
```

Pass the current file name as a parameter.

------------------------------------------------------------------------

### Practice 10 --- Mapping Table

Replace the hard-coded array with:

``` text
mapping table
      ↓
SQL query
      ↓
For Each
      ↓
ingestion notebook
```

This is the most important advanced pattern from the transcript.

------------------------------------------------------------------------

# 71. Final Mental Model

If you remember only one thing, remember this:

``` text
                 LAKEFLOW JOB
                      |
          ┌───────────┼───────────┐
          ↓           ↓           ↓
       TASKS      CONDITIONS    LOOPS
          |           |           |
          ↓           ↓           ↓
   Dependencies    If/Else      For Each
          |
          ↓
     Parallelism
          |
          ↓
   Dynamic Values
          |
          ↓
     Parameters
          |
          ↓
      SQL Output
          |
          ↓
   Dynamic Processing
          |
          ↓
    Monitoring/Alerts
          |
          ↓
    Schedule/Triggers
          |
          ↓
     Notifications
```

The overall idea is:

> **Lakeflow Jobs turns individual pieces of Databricks work into a
> controlled, parameterized, repeatable, monitorable workflow.**

The most important progression is:

``` text
Job
→ Task
→ Dependency
→ Parallelism
→ Condition
→ Loop
→ Parameter
→ Task Value
→ Dynamic Reference
→ SQL Output
→ Dynamic Ingestion
→ Mapping Table
→ Scheduling
→ Monitoring
```
