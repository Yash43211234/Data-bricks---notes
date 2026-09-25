# Databricks Auto Loader

## 1. What is Auto Loader?

**Auto Loader** is a Databricks utility used to **incrementally and efficiently ingest new files** arriving in cloud storage.

It can ingest files from:

* **AWS S3**
* **Azure ADLS**
* **GCP GCS**
* **DBFS / Volumes**

### Main benefits

* Processes **new files incrementally**
* Avoids processing the same file repeatedly
* Supports **exactly-once processing**
* Can scale to **millions of files**
* Supports **schema evolution**
* Supports both:

  * Streaming mode
  * Batch-like processing using `availableNow`
* Works as a **Structured Streaming source**

---

# 2. Auto Loader and Structured Streaming

Auto Loader provides a Structured Streaming source called:

```text
cloudFiles
```

Therefore, Auto Loader commonly uses:

```python
spark.readStream
```

Basic structure:

```python
df = (
    spark.readStream
    .format("cloudFiles")
    ...
)
```

### Important idea

> **Auto Loader = Structured Streaming + efficient file discovery**

---

# 3. Why Use Auto Loader?

Suppose files continuously arrive in cloud storage:

```text
landing/
├── 2026/
│   ├── 01/
│   │   ├── 01/
│   │   │   └── orders.csv
│   │   ├── 02/
│   │   │   └── orders.csv
│   │   └── 03/
│   │       └── orders.csv
```

Instead of repeatedly reading every file, Auto Loader keeps track of which files have already been processed.

When a new file arrives:

```text
orders_day_04.csv
```

Auto Loader processes **only the new file**.

---

# 4. File Detection Modes

Auto Loader provides two major ways to discover new files.

## 4.1 Directory Listing

This is the **default** mode.

Auto Loader periodically checks the storage location using cloud storage APIs.

Conceptually:

```text
Cloud Storage
      ↓
Directory Listing
      ↓
Find new files
      ↓
Auto Loader
```

### Characteristics

* Default mode
* Uses cloud storage APIs
* Easy to configure
* No additional cloud notification infrastructure required

Auto Loader maintains information about processed files in its checkpoint/state information.

---

# 4.2 File Notification

In file notification mode, Auto Loader uses cloud notification and queueing services.

Conceptually:

```text
New File
   ↓
Cloud Storage
   ↓
Notification Service
   ↓
Queue
   ↓
Auto Loader
```

When a new file arrives:

1. Cloud storage generates a notification.
2. Notification is placed into a queue.
3. Auto Loader reads the notification.
4. Auto Loader processes the new file.

### Important

File notification may require **additional cloud permissions/privileges** because Databricks may need to configure notification and queueing resources.

---

# 5. Exactly-Once Processing

One of the important benefits of Auto Loader is its ability to process files without repeatedly ingesting the same files.

Example:

### First run

```text
file1.csv
file2.csv
file3.csv
```

All three files are processed.

### Second run

No new files:

```text
file1.csv  → already processed
file2.csv  → already processed
file3.csv  → already processed
```

They are not processed again.

### New file arrives

```text
file4.csv
```

Next run:

```text
file4.csv → processed
```

Therefore:

```text
Existing files → skipped
New files      → processed
```

---

# 6. Checkpoint Location

Auto Loader uses a **checkpoint location** to maintain information required for incremental processing.

Example:

```text
checkpoint/
└── autoloader/
```

The checkpoint helps Auto Loader keep track of processing state.

### Important

Checkpointing is important for:

* Tracking processed files
* Maintaining streaming state
* Supporting fault tolerance
* Supporting exactly-once processing

---

# 7. RocksDB

Auto Loader uses **RocksDB** internally as part of its scalable file/state tracking mechanism.

Conceptually:

```text
Auto Loader
     ↓
Checkpoint
     ↓
RocksDB
     ↓
Processed-file/state information
```

RocksDB is designed to efficiently manage large amounts of state.

---

# 8. Basic Auto Loader Syntax

A basic Auto Loader DataFrame:

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("pathGlobFilter", "*.csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .load(input_path)
)
```

---

# 9. Important Auto Loader Options

| Option                           | Purpose                                |
| -------------------------------- | -------------------------------------- |
| `format("cloudFiles")`           | Uses Auto Loader                       |
| `cloudFiles.format`              | Specifies input file format            |
| `pathGlobFilter`                 | Filters files by pattern               |
| `header`                         | Indicates whether CSV contains headers |
| `cloudFiles.schemaLocation`      | Stores Auto Loader schema information  |
| `cloudFiles.schemaEvolutionMode` | Controls schema evolution              |
| `cloudFiles.useNotifications`    | Enables file notification mode         |

---

# 10. `cloudFiles.format`

Specifies the format of the incoming files.

For CSV:

```python
.option("cloudFiles.format", "csv")
```

Other common formats include:

```text
csv
json
parquet
avro
text
binaryFile
```

---

# 11. `pathGlobFilter`

Used to filter files based on their filename/extension.

Example:

```python
.option("pathGlobFilter", "*.csv")
```

This means:

> Read only CSV files.

---

# 12. Reading Nested Folder Structures

Suppose the input location contains:

```text
landing/
├── 2026/
│   ├── 01/
│   │   ├── 01/
│   │   │   └── orders.csv
│   │   ├── 02/
│   │   │   └── orders.csv
│   │   └── 03/
│   │       └── orders.csv
```

Auto Loader can process files from nested structures.

The important idea is that the **root input location** is supplied to Auto Loader, rather than manually specifying every date folder.

---

# 13. Schema Location

Auto Loader maintains schema information separately from the data.

Example:

```python
.option(
    "cloudFiles.schemaLocation",
    "/checkpoint/autoloader/1"
)
```

### Why is schema location needed?

It helps Auto Loader:

* Store inferred schema
* Track schema changes
* Support schema evolution
* Maintain schema information across runs

### Difference

**Checkpoint location**

```text
Tracks processing/state information
```

**Schema location**

```text
Tracks Auto Loader schema information
```

They are related but serve different purposes.

---

# 14. Schema Inference

Suppose the CSV contains:

```text
order_id,customer_id,quantity,unit_price
```

Auto Loader can infer the schema.

However, inferred types may not always be the desired types.

For example:

```text
quantity   → string
unit_price → string
```

even though we want:

```text
quantity   → integer
unit_price → double
```

---

# 15. Schema Hints

Instead of explicitly defining the entire schema, we can provide hints for specific columns.

Example:

```python
.option(
    "cloudFiles.schemaHints",
    "quantity INT, unit_price DOUBLE"
)
```

This tells Auto Loader:

```text
quantity   → INT
unit_price → DOUBLE
```

### Why use schema hints?

Useful when:

* Most schema can be inferred automatically
* Only a few columns need specific data types
* You don't want to manually define the complete schema

---

# 16. Writing Auto Loader Data

Because Auto Loader is based on Structured Streaming, use:

```python
df.writeStream
```

Example:

```python
query = (
    df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("dev_bronze_invoice")
)
```

---

# 17. `availableNow=True`

A very useful trigger for Auto Loader is:

```python
.trigger(availableNow=True)
```

This allows Auto Loader to behave like a **batch processing job**:

```text
Start job
   ↓
Process currently available files
   ↓
Finish processing
   ↓
Stop
```

This is different from a continuously running stream.

### Useful for

* Scheduled ingestion jobs
* Incremental batch pipelines
* Databricks Jobs
* Processing newly arrived files periodically

---

# 18. Continuous Streaming

Auto Loader can also run continuously using a processing-time trigger.

Conceptually:

```text
Start stream
     ↓
Wait for files
     ↓
Process new files
     ↓
Wait
     ↓
Process new files
     ↓
...
```

So Auto Loader can support both:

```text
Batch-like processing
        ↓
availableNow

Continuous processing
        ↓
processingTime
```

---

# 19. Adding Source File Name

It is often useful to know **which file produced each record**.

Databricks provides metadata that can be used to retrieve the source filename.

Conceptually:

```python
from pyspark.sql.functions import col

df = df.withColumn(
    "file",
    col("_metadata.file_name")
)
```

Now the output contains:

```text
order_id
customer_id
quantity
unit_price
file
```

Example:

```text
1001 | C01 | 5 | 20.5 | orders_01.csv
1002 | C02 | 2 | 15.0 | orders_01.csv
1003 | C03 | 8 | 12.0 | orders_02.csv
```

This is very useful for **debugging and data lineage**.

---

# 20. Testing Incremental Processing

Suppose the first run has:

```text
file1.csv
file2.csv
file3.csv
```

Auto Loader processes:

```text
file1
file2
file3
```

Now add:

```text
file4.csv
```

Run Auto Loader again.

It processes:

```text
file4.csv
```

It does **not** reprocess:

```text
file1.csv
file2.csv
file3.csv
```

Therefore:

> Auto Loader performs incremental file ingestion.

---

# 21. Schema Evolution

Real-world data can change over time.

Initially:

```text
order_id
customer_id
quantity
unit_price
```

Later, a new file arrives:

```text
order_id
customer_id
quantity
unit_price
state
```

The new column is a **schema change**.

Auto Loader provides several schema evolution modes.

---

# 22. Schema Evolution Modes

Important modes:

1. `addNewColumns`
2. `rescue`
3. `none`
4. `failOnNewColumns`

---

# 23. `addNewColumns`

This is the default behavior when Auto Loader is inferring the schema and no explicit schema is provided.

Example:

Original schema:

```text
order_id
customer_id
quantity
unit_price
```

New file:

```text
order_id
customer_id
quantity
unit_price
state
```

Auto Loader detects:

```text
state
```

and updates its schema information.

Conceptually:

```text
Old Schema
    ↓
New column detected
    ↓
Schema updated
    ↓
Stream may need to be restarted
```

### Important behavior

When a new column is detected, the current streaming run can fail with a schema-related exception.

Auto Loader updates the schema location.

After restarting the stream with the updated schema, the new column can be processed.

---

# 24. Delta Table Schema Evolution

When the target Delta table also needs to accept the new column, schema evolution may need to be enabled for the write.

For example:

```python
.option("mergeSchema", "true")
```

Conceptually:

```text
Auto Loader schema
        ↓
New column detected
        ↓
Delta table schema
        ↓
mergeSchema = true
        ↓
New column added
```

---

# 25. `rescue`

Example:

```python
.option(
    "cloudFiles.schemaEvolutionMode",
    "rescue"
)
```

In rescue mode, unexpected/new columns are not allowed to break the stream.

Instead, the unexpected data is placed into a special rescue column.

Conceptually:

```text
New column
    ↓
Not part of expected schema
    ↓
_rescued_data
```

Example:

```text
order_id | quantity | _rescued_data
------------------------------------
1001     | 5        | null
1002     | 3        | {"state":"CA"}
```

### Benefit

The stream can continue processing instead of failing.

### Useful when

You don't want a schema change to stop your pipeline.

---

# 26. `none`

Example:

```python
.option(
    "cloudFiles.schemaEvolutionMode",
    "none"
)
```

In this mode, schema changes are ignored.

Suppose a new column appears:

```text
state
```

Auto Loader does not add it to the inferred schema.

The stream continues.

Conceptually:

```text
New column
    ↓
Ignored
    ↓
Stream continues
```

There is no automatic rescue of the new column unless rescue behavior is explicitly configured.

---

# 27. `failOnNewColumns`

Example:

```python
.option(
    "cloudFiles.schemaEvolutionMode",
    "failOnNewColumns"
)
```

If a new column appears:

```text
Existing schema
      +
New column
      ↓
Stream FAILS
```

This is useful when schema changes should be treated as an error.

The pipeline must then be manually handled by updating the schema/configuration before processing continues.

---

# 28. Schema Evolution Comparison

| Mode               | New Column                              | Stream              |
| ------------------ | --------------------------------------- | ------------------- |
| `addNewColumns`    | Adds to schema                          | May require restart |
| `rescue`           | Stores unexpected data in rescue column | Continues           |
| `none`             | Ignores schema change                   | Continues           |
| `failOnNewColumns` | Treats it as an error                   | Fails               |

### Easy way to remember

```text
addNewColumns
→ Add it

rescue
→ Save it somewhere

none
→ Ignore it

failOnNewColumns
→ Stop
```

---

# 29. Schema Location vs Checkpoint Location

This is an important interview distinction.

### Schema Location

```python
.option(
    "cloudFiles.schemaLocation",
    schema_path
)
```

Used primarily for:

* Schema inference
* Schema tracking
* Schema evolution

### Checkpoint Location

```python
.option(
    "checkpointLocation",
    checkpoint_path
)
```

Used for:

* Streaming state
* Progress tracking
* File processing state
* Fault tolerance
* Exactly-once processing

### Remember

```text
Schema Location
→ "What is the schema?"

Checkpoint
→ "What has already been processed?"
```

---

# 30. File Notification Configuration

To use file notification mode:

```python
.option(
    "cloudFiles.useNotifications",
    "true"
)
```

Conceptually:

```text
useNotifications = true
```

changes the file discovery approach from:

```text
Directory Listing
```

to:

```text
File Notifications
```

### Important

This may require elevated cloud permissions because notification and queueing services need to be configured.

---

# 31. Complete Example

```python
from pyspark.sql.functions import col

input_path = "/Volumes/workspace/default/landing/autoloader_input"
checkpoint_path = "/Volumes/workspace/default/checkpoint/autoloader/1"

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("pathGlobFilter", "*.csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .option(
        "cloudFiles.schemaHints",
        "quantity INT, unit_price DOUBLE"
    )
    .load(input_path)
)

df = df.withColumn(
    "file",
    col("_metadata.file_name")
)

query = (
    df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("dev_bronze_invoice")
)
```

---

# 32. Auto Loader Architecture

```text
                 Cloud Storage
              ┌──────┬──────┐
              │      │      │
             S3    ADLS    GCS
              │      │      │
              └──────┼──────┘
                     ↓
                Auto Loader
                cloudFiles
                     ↓
             File Detection
              ┌──────┴──────┐
              │             │
       Directory Listing   Notification
              │             │
              └──────┬──────┘
                     ↓
              Structured Streaming
                     ↓
              Schema Handling
                     ↓
              Bronze Delta Table
```

---

# 33. Typical Auto Loader Pipeline

```text
Source Files
     ↓
Cloud Storage
     ↓
Auto Loader
     ↓
File Detection
     ↓
Schema Inference / Schema Hints
     ↓
Schema Evolution
     ↓
Structured Streaming
     ↓
Bronze Delta Table
```

---

# 34. Important Interview Points

### Q1. What is Auto Loader?

> Auto Loader is a Databricks utility for incrementally and efficiently ingesting new files from cloud storage using Structured Streaming.

### Q2. What is the Structured Streaming source used by Auto Loader?

```text
cloudFiles
```

### Q3. What are the two file detection modes?

```text
1. Directory Listing
2. File Notification
```

### Q4. Which is the default?

```text
Directory Listing
```

### Q5. How does Auto Loader know which files were already processed?

Using checkpoint/state information.

### Q6. What is `cloudFiles.schemaLocation`?

It is the location where Auto Loader stores schema information used for schema inference and evolution.

### Q7. What are schema evolution modes?

```text
addNewColumns
rescue
none
failOnNewColumns
```

### Q8. What does `rescue` do?

It puts unexpected/new data into the rescue column instead of failing the stream.

### Q9. What does `none` do?

It ignores schema changes.

### Q10. What does `failOnNewColumns` do?

It fails the stream when new columns are detected.

### Q11. How can Auto Loader process files like a batch?

Use:

```python
.trigger(availableNow=True)
```

### Q12. How can you specify data types for selected columns?

Use:

```python
cloudFiles.schemaHints
```

### Q13. How can you capture the source filename?

Use:

```python
_metadata.file_name
```

---

# 35. Auto Loader vs `COPY INTO`

| Feature                      | Auto Loader | COPY INTO                      |
| ---------------------------- | ----------- | ------------------------------ |
| Incremental ingestion        | ✅           | ✅                              |
| Structured Streaming         | ✅           | ❌                              |
| Continuous ingestion         | ✅           | ❌                              |
| Schema evolution             | ✅           | More limited                   |
| Large-scale file discovery   | ✅           | Suitable for simpler workloads |
| File notification            | ✅           | ❌                              |
| Batch-style ingestion        | ✅           | ✅                              |
| Exactly-once file processing | ✅           | ✅                              |

### Simple distinction

```text
COPY INTO
→ Simple incremental file loading

Auto Loader
→ Scalable incremental file ingestion + streaming + schema evolution
```

---

# 36. Key Takeaways

Remember these points:

```text
Auto Loader
    ↓
cloudFiles
    ↓
Structured Streaming
    ↓
Incremental file ingestion
```

### File discovery

```text
Directory Listing → Default
File Notification → Notification + Queue
```

### State

```text
Checkpoint
    ↓
Tracks processing/state
```

### Schema

```text
Schema Location
    ↓
Stores schema information
```

### Schema evolution

```text
addNewColumns → Add new columns
rescue        → Rescue unexpected data
none          → Ignore changes
failOnNewColumns → Fail
```

### File types

```text
CSV / JSON / Parquet / etc.
```

### Processing modes

```text
availableNow → Process available files and stop
processingTime → Continuous/periodic streaming
```

### Most important mental model

> **Auto Loader continuously or incrementally watches a cloud storage location, discovers new files, processes only the files that have not already been processed, and uses checkpointing and schema management to make ingestion scalable and reliable.**
