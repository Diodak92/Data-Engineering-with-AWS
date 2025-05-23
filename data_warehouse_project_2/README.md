
# 🎧 Sparkify Data Warehouse Project

## 📌 Project Overview

**Sparkify**, a fast-growing music streaming startup, has expanded its user base and song catalog. To support better scalability and deeper analytics, Sparkify is migrating its data and processes to the cloud.

Their raw data lives in **Amazon S3**:
- A directory of **JSON logs** tracking user activity in the app
- A directory of **JSON files** containing metadata about the songs available

The objective of this project is to build an **ETL pipeline** that:
1. **Extracts** data from S3
2. **Stages** it in **Amazon Redshift**
3. **Transforms** the data into a set of **dimensional tables**

These tables will support Sparkify’s analytics team in discovering insights about what songs users are listening to, how often, and by whom.

---

## 🗂️ Project Structure

| File | Description |
|------|-------------|
| `create_tables.py` | Creates all the necessary **staging** and **analytical tables** in Redshift |
| `etl.py` | The core **ETL pipeline**. Loads raw data from S3 into staging tables in Redshift, then transforms and inserts it into dimensional tables |
| `sql_queries.py` | Contains all the **SQL statements** used by the project (table creation, inserts, copies, etc.) |
| `dwh.cfg` | Configuration file storing **AWS credentials**, **Redshift cluster details**, and **S3 paths** |
| `s3_utils.ipynb` | A helper notebook with utility functions to **explore the S3 bucket** contents |

---

## ▶️ How to Run the Project

1. **Install Required Packages**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configure AWS and Redshift**
   - Open `dwh.cfg` and update it with:
     - Your **AWS Access Key** and **Secret**
     - Your **IAM Role ARN**
     - Your **Redshift cluster endpoint** and credentials

3. **🚀 Launch Redsift cluster**
   ```bash
   python create_redshift_cluster.py
   ```
4. **Create Tables in Redshift**
   Run this script to create the necessary tables:
   ```bash
   python create_tables.py
   ```

5. **Run the ETL Pipeline**
   This script loads data from S3 into Redshift and transforms it into a star schema:
   ```bash
   python etl.py
   ```

---

## ✅ Final Notes

Once everything is set up and run, your Redshift database will contain a star schema with fact and dimension tables ready for analysis — enabling Sparkify to dive into their user listening patterns with ease.
