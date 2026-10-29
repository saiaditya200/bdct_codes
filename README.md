# BDCT Lab Internal — Cloudera Exam-Ready Commands

Quick reference for the Big Data Computing Technologies (BDCT) lab, written for a **Cloudera Hadoop environment** such as Cloudera QuickStart VM / CDH. Run shell commands in the Cloudera Linux terminal; Hive SQL inside Hive/Beeline; Pig statements in a Pig script or Grunt shell; MySQL statements in the MySQL client.

> **Cloudera notes**
> - The examples use the common Cloudera VM account `cloudera` and HDFS paths under `/user/cloudera`. If your lab uses another Linux account, update the paths consistently.
> - Start the Hadoop services from Cloudera Manager (or the VM's service controls) before running commands. Do not run `start-dfs.sh` / `start-yarn.sh` blindly on a managed cluster.
> - CDH releases differ. Some older Cloudera systems use Java 7/8, older Hive/Pig/Sqoop syntax, and `hive` rather than Beeline. Use the launcher available in your lab.
> - Commands below are templates for a standard lab setup; they cannot guarantee that services, permissions, JDBC drivers, or Sqoop are configured on every cluster.

## Syllabus Index
1. [HDFS Storage](#1-hdfs-storage)
2. [MapReduce — Word Count](#2-mapreduce--word-count)
3. [Most Frequent Words](#3-most-frequent-words)
4. [Hive Data Processing](#4-hive-data-processing)
5. [Pig Log Processing](#5-pig-log-processing)
6. [Sqoop Import and Export](#6-sqoop-import-and-export)

---

## 1. HDFS Storage

### Create local files, create an HDFS directory, upload and read files

Run in the Cloudera Linux terminal:

```bash
whoami
hadoop version
hdfs dfs -mkdir -p /user/cloudera/bdct/input
mkdir -p ~/bdct_lab
cd ~/bdct_lab
printf 'hello hadoop\n' > file1.txt
printf 'big data lab\n' > file2.txt
hdfs dfs -put -f file1.txt file2.txt /user/cloudera/bdct/input/
hdfs dfs -ls /user/cloudera/bdct/input
hdfs dfs -cat /user/cloudera/bdct/input/file1.txt
```

### Copy a file from HDFS to local filesystem

```bash
hdfs dfs -get -f /user/cloudera/bdct/input/file1.txt ~/bdct_lab/downloaded_file.txt
cat ~/bdct_lab/downloaded_file.txt
```

### Useful HDFS commands

| Operation | Command |
|---|---|
| Create directory | `hdfs dfs -mkdir -p /path` |
| Upload local file | `hdfs dfs -put file.txt /path/` |
| List files | `hdfs dfs -ls /path` |
| Display file | `hdfs dfs -cat /path/file.txt` |
| Download file | `hdfs dfs -get /path/file.txt .` |
| Copy inside HDFS | `hdfs dfs -cp /src /dest` |
| Move inside HDFS | `hdfs dfs -mv /src /dest` |
| Delete file | `hdfs dfs -rm /path/file.txt` |
| Delete directory | `hdfs dfs -rm -r /path` |

---

## 2. MapReduce — Word Count

**Aim:** Count every word in a text file using Java MapReduce.

### A. Create and upload input

```bash
mkdir -p ~/bdct_wordcount
cd ~/bdct_wordcount
printf 'big data big hadoop\nhadoop big data\n' > input.txt
hdfs dfs -mkdir -p /user/cloudera/wordcount/input
hdfs dfs -put -f input.txt /user/cloudera/wordcount/input/
```

### B. Save this program as `WordCount.java`

```java
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
    public static class MapClass extends Mapper<Object, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer st = new StringTokenizer(value.toString());
            while (st.hasMoreTokens()) {
                word.set(st.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordCount <input> <output>");
            System.exit(2);
        }
        Job job = Job.getInstance(new Configuration(), "Word Count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
```

### C. Compile, package, execute

Run from `~/bdct_wordcount`:

```bash
cd ~/bdct_wordcount
mkdir -p classes
javac -classpath "$(hadoop classpath)" -d classes WordCount.java
jar -cvf wordcount.jar -C classes .
hdfs dfs -rm -r -f /user/cloudera/wordcount/output
hadoop jar wordcount.jar WordCount /user/cloudera/wordcount/input /user/cloudera/wordcount/output
hdfs dfs -cat /user/cloudera/wordcount/output/part-r-00000
```

Expected output (order may vary):

```text
big       3
data      2
hadoop    2
```

**Remember:** Mapper emits `(word, 1)`; Reducer adds values for each word. The output directory must not exist when a MapReduce job starts. If `javac` or `hadoop classpath` is unavailable, check that the Hadoop client/development packages are installed in the Cloudera environment.

---

## 3. Most Frequent Words

**Aim:** Sort Word Count results by frequency, highest first. First complete Experiment 2.

```bash
hdfs dfs -cat /user/cloudera/wordcount/output/part-r-00000 | sort -k2,2nr
```

Display the top result:

```bash
hdfs dfs -cat /user/cloudera/wordcount/output/part-r-00000 | sort -k2,2nr | head -1
```

Save a sorted copy locally:

```bash
hdfs dfs -cat /user/cloudera/wordcount/output/part-r-00000 | sort -k2,2nr > ~/bdct_wordcount/frequent_words.txt
cat ~/bdct_wordcount/frequent_words.txt
```

Expected top word for the sample: `big 3`.

> This is a simple lab method using Linux `sort` on MapReduce output. If the question specifically requires the most-frequent-word result as a second MapReduce job, a separate second job is needed.

---

## 4. Hive Data Processing

**Aim:** Create a table with `data`, `user_id`, and `properties`; calculate minimum, maximum and distinct ages; count records by state.

### A. Open Hive

From the Cloudera terminal, use the launcher available on your CDH version:

```bash
hive
```

If your cluster uses HiveServer2/Beeline instead, connect with the JDBC URL supplied by your instructor, for example:

```bash
beeline -u 'jdbc:hive2://localhost:10000/default' -n cloudera
```

### B. Run the following HiveQL

```sql
CREATE DATABASE IF NOT EXISTS bdct;
USE bdct;

CREATE TABLE IF NOT EXISTS user_data (
  data STRING,
  user_id STRING,
  properties STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

INSERT INTO TABLE user_data VALUES
('record1','U1','Age=21;state=CA;gender=M'),
('record2','U2','Age=25;state=TX;gender=F'),
('record3','U3','Age=21;state=CA;gender=F'),
('record4','U4','Age=30;state=NY;gender=M');

SELECT * FROM user_data;

SELECT
  MIN(CAST(REGEXP_EXTRACT(properties, 'Age=([0-9]+)', 1) AS INT)) AS min_age,
  MAX(CAST(REGEXP_EXTRACT(properties, 'Age=([0-9]+)', 1) AS INT)) AS max_age,
  COUNT(DISTINCT REGEXP_EXTRACT(properties, 'Age=([0-9]+)', 1)) AS unique_ages
FROM user_data;

SELECT
  REGEXP_EXTRACT(properties, 'state=([^;]+)', 1) AS state,
  COUNT(*) AS total
FROM user_data
GROUP BY REGEXP_EXTRACT(properties, 'state=([^;]+)', 1);
```

Expected: minimum age = 21, maximum age = 30, distinct ages = 3; state counts CA = 2, NY = 1, TX = 1.

Exit the Hive CLI with:

```sql
QUIT;
```

### Alternative: load a local text file

Create a tab-separated file with exactly three fields matching the table, then in Hive:

```sql
LOAD DATA LOCAL INPATH '/home/cloudera/user_data.txt' INTO TABLE user_data;
```

Replace the path with the actual file path. Do not load the same records after inserting them unless duplicate rows are intended. On older Hive versions, `INSERT ... VALUES` support can differ; if unavailable, use the tab-separated file and `LOAD DATA LOCAL INPATH`.

---

## 5. Pig Log Processing

**Aim:** Calculate page views per day, per hour across all dates, and per site. Input columns: `site, hour_of_day, page_views, date`.

### A. Create sample input and upload to HDFS

```bash
mkdir -p ~/bdct_pig
cd ~/bdct_pig
cat > logs.txt <<'EOF'
site1	9	100	2026-10-27
site2	9	150	2026-10-27
site1	10	200	2026-10-27
site1	9	120	2026-10-28
site2	10	180	2026-10-28
EOF
hdfs dfs -mkdir -p /user/cloudera/data/pig/simplelogs
hdfs dfs -put -f logs.txt /user/cloudera/data/pig/simplelogs/
hdfs dfs -ls /user/cloudera/data/pig/simplelogs
```

### B. Save as `logs.pig`

```pig
logs = LOAD '/user/cloudera/data/pig/simplelogs/logs.txt'
USING PigStorage('\t')
AS (site:chararray, hour_of_day:int, page_views:int, date:chararray);

by_day = GROUP logs BY date;
daily_views = FOREACH by_day GENERATE group AS date, SUM(logs.page_views) AS total_views;
DUMP daily_views;

by_hour = GROUP logs BY hour_of_day;
hourly_views = FOREACH by_hour GENERATE group AS hour, SUM(logs.page_views) AS total_views;
DUMP hourly_views;

by_site = GROUP logs BY site;
site_views = FOREACH by_site GENERATE group AS site, SUM(logs.page_views) AS total_views;
DUMP site_views;
```

The path uses Cloudera's common `cloudera` account. If your HDFS username differs, update the path in the script.

### C. Execute

```bash
cd ~/bdct_pig
pig logs.pig
```

Expected totals:

| Date | Views |
|---|---:|
| 2026-10-27 | 450 |
| 2026-10-28 | 300 |

| Hour | Views |
|---|---:|
| 9 | 370 |
| 10 | 380 |

| Site | Views |
|---|---:|
| site1 | 420 |
| site2 | 330 |

Pig commands to remember: `LOAD`, `GROUP`, `FOREACH ... GENERATE`, `SUM`, `DUMP`.

---

## 6. Sqoop Import and Export

**Aim:** Import a MySQL table into HDFS/Hive and export HDFS records back to MySQL.

> Sqoop may not be installed in every CDH deployment. These commands require Sqoop, a compatible MySQL JDBC driver, network access to MySQL, and permission to write the target paths. Use the database hostname, username and password configured by your instructor.

### A. Create MySQL database and source table

From the Cloudera terminal:

```bash
mysql -u root -p
```

In the MySQL prompt:

```sql
CREATE DATABASE IF NOT EXISTS bdctdb;
USE bdctdb;
CREATE TABLE students (id INT, name VARCHAR(50), marks INT);
INSERT INTO students VALUES (1,'Aditya',85),(2,'Rahul',90),(3,'Priya',88);
SELECT * FROM students;
EXIT;
```

If the table already exists, use a fresh table name or clear it before inserting the sample records.

### B. Import MySQL table into HDFS

Run in the Cloudera terminal; enter the MySQL password when prompted:

```bash
sqoop import \
  --connect jdbc:mysql://localhost:3306/bdctdb \
  --username root -P \
  --table students \
  --target-dir /user/cloudera/sqoop/students \
  --num-mappers 1
```

Some older Sqoop builds use `--m 1` instead of `--num-mappers 1`. Check `sqoop help import` if the option differs.

Verify:

```bash
hdfs dfs -ls /user/cloudera/sqoop/students
hdfs dfs -cat /user/cloudera/sqoop/students/part-m-00000
```

### C. Import directly into Hive

Ensure database `bdct` exists in Hive first. Then run:

```bash
sqoop import \
  --connect jdbc:mysql://localhost:3306/bdctdb \
  --username root -P \
  --table students \
  --hive-import \
  --hive-database bdct \
  --hive-table students_sqoop \
  --create-hive-table \
  --num-mappers 1
```

Verify in Hive:

```bash
hive -e "SELECT * FROM bdct.students_sqoop;"
```

If `--num-mappers` is unsupported on your version, use `--m 1`. If the target Hive table already exists, select a new target name or follow the lab's existing-table import procedure.

### D. Export HDFS data to MySQL

Create the destination table in MySQL:

```sql
USE bdctdb;
CREATE TABLE students_export (id INT, name VARCHAR(50), marks INT);
```

Prepare a tab-separated text file in the Cloudera terminal and upload it:

```bash
printf '4\tKiran\t80\n5\tSneha\t92\n' > ~/export.txt
hdfs dfs -mkdir -p /user/cloudera/sqoop/export
hdfs dfs -put -f ~/export.txt /user/cloudera/sqoop/export/
```

Run export:

```bash
sqoop export \
  --connect jdbc:mysql://localhost:3306/bdctdb \
  --username root -P \
  --table students_export \
  --export-dir /user/cloudera/sqoop/export \
  --input-fields-terminated-by '\t' \
  --num-mappers 1
```

Verify in MySQL:

```sql
SELECT * FROM students_export;
```

If Sqoop cannot connect to `localhost`, use the MySQL host reachable from the cluster nodes. Never place a real password directly in a command or commit it to GitHub.

---

## Last-Minute Revision

| Experiment | Memorize |
|---|---|
| HDFS | `-mkdir`, `-put`, `-ls`, `-cat`, `-get` |
| Word Count | Mapper emits `(word,1)`; Reducer sums values |
| Frequent Words | Sort count column descending |
| Hive | `CREATE TABLE`, `REGEXP_EXTRACT`, `MIN`, `MAX`, `COUNT(DISTINCT)`, `GROUP BY` |
| Pig | `LOAD`, `GROUP`, `FOREACH`, `SUM`, `DUMP` |
| Sqoop | `sqoop import`, `--hive-import`, `sqoop export` |

## Cloudera Environment Checks

Run in the terminal:

```bash
whoami
hadoop version
hdfs dfs -ls /
java -version
hive --version
pig -version
sqoop version
```

If a command is not found, the corresponding client may not be installed or its environment may not be configured on that cluster.
