As we saw in the previous video:

- What Databricks is
- How to create a free account
- And how to explore the Databricks platform

## 🎯 In this video

We will learn:

👉 How to use the SQL Editor in Databricks  
👉 How to write SQL queries  
👉 How to interact with the database  
👉 How to get insights and perform transformations on data  

---

## 🔹 Opening SQL Editor

Now let's go to the screen and understand it.

1. Go to the **SQL** tab
2. Click on **SQL Editor**
3. A new SQL editor will open

👉 It looks similar to:

- MySQL
- PostgreSQL

---

## 🔹 Creating a New Notebook

- Create a new notebook
- Now you can start writing SQL queries

---

## ⚠️ Important Step (Very Important)

Before running any query:

👉 You must **connect/start Compute**

**Why?**  
Because compute is the machine that runs your queries.  
If it is not connected → query will not run.

✔️ So:

- Select compute
- Click **Start**

---

## 🔹 Creating Database

```sql
CREATE DATABASE IF NOT EXISTS youtube_db;
```
👉 Output: Shows OK → means query executed successfully

🔹 Selecting Database
sql
USE youtube_db;
👉 Now you are inside this database

🔹 Checking Tables
sql
SHOW TABLES;
👉 Output: Empty (because no table exists yet)

⚠️ Important Concept: IF NOT EXISTS
If you run:

sql
CREATE DATABASE youtube_db;
👉 It will give error if DB already exists

✔️ So we use:

sql
IF NOT EXISTS
👉 To avoid errors

🔹 Creating Table
sql
CREATE TABLE demo (
    id INT,
    name STRING
);
👉 Table created successfully

🔹 Inserting Data
sql
INSERT INTO demo (id, name)
VALUES
(1, 'John'),
(2, 'Jay');
👉 Output: Rows inserted = 2

🔹 Fetching Data
sql
SELECT * FROM demo;
👉 Output: Shows inserted data

🔹 Checking in Catalog
Go to Catalog

Open your database → youtube_db

You will see: Table → demo

👉 Click on table → you can see:

Columns

Data types

Metadata

Tags

🔹 Important Concept (Delta)
👉 Data source shows Delta, not table

Why?
Because Databricks uses Delta Lake (Lakehouse architecture)

🔚 Final Summary
In this video, we learned:

How to open SQL Editor

How to connect compute

How to create database

How to create table

How to insert data

How to fetch data

🚀 One-Line Summary
👉 Databricks SQL Editor is used to write queries, manage data, and get insights easily


