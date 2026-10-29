# BDCT Lab Internal — Exam-Ready Commands

Quick reference for the Big Data Computing Technologies lab. Shell commands run in the Linux terminal; Hive SQL runs inside Hive; Pig code runs as a script. These examples assume a Linux Hadoop environment with the required services and tools installed. Adjust paths and credentials to match your college lab.

## Syllabus Index
1. [HDFS Storage](#1-hdfs-storage)
2. [MapReduce — Word Count](#2-mapreduce--word-count)
3. [Most Frequent Words](#3-most-frequent-words)
4. [Hive Data Processing](#4-hive-data-processing)
5. [Pig Log Processing](#5-pig-log-processing)
6. [Sqoop Import and Export](#6-sqoop-import-and-export)

---

## 1. HDFS Storage

### Create sample local files, create HDFS directory, upload files

```bash
mkdir -p ~/bdct_lab
cd ~/bdct_lab
printf 'hello hadoop\n' > file1.txt
printf 'big data lab\n' > file2.txt
hdfs dfs -mkdir -p /user/$USER/bdct/input
hdfs dfs -put -f file1.txt file2.txt /user/$USER/bdct/input/
hdfs dfs -ls /user/$USER/bdct/input
hdfs dfs -cat /user/$USER/bdct/input/file1.txt
```

### Copy a file from HDFS to local filesystem

```bash
hdfs dfs -get -f /user/$USER/bdct/input/file1.txt ./downloaded_file.txt
cat downloaded_file.txt
```

### Useful HDFS commands

| Operation | Command |
|---|---|
| Create directory | `hdfs dfs -mkdir -p /path` |
| Upload file | `hdfs dfs -put file.txt /path/` |
| List | `hdfs dfs -ls /path` |
| Display file | `hdfs dfs -cat /path/file.txt` |
| Download | `hdfs dfs -get /path/file.txt .` |
| Copy in HDFS | `hdfs dfs -cp /src /dest` |
| Move in HDFS | `hdfs dfs -mv /src /dest` |
| Delete file | `hdfs dfs -rm /path/file.txt` |
| Delete directory | `hdfs dfs -rm -r /path` |

---

## 2. MapReduce — Word Count

**Aim:** Count each word using Java MapReduce.

### Create and upload input

```bash
mkdir -p ~/bdct_wordcount
cd ~/bdct_wordcount
printf 'big data big hadoop\nhadoop big data\n' > input.txt
hdfs dfs -mkdir -p /user/$USER/wordcount/input
hdfs dfs -put -f input.txt /user/$USER/wordcount/input/
```

### Save as `WordCount.java`

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
            for (IntWritable value : values) sum += value.get();
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

### Compile, package, execute

Run from the directory containing `WordCount.java`:

```bash
mkdir -p classes
javac -classpath "$(hadoop classpath)" -d classes WordCount.java
jar -cvf wordcount.jar -C classes .
hdfs dfs -rm -r -f /user/$USER/wordcount/output
hadoop jar wordcount.jar WordCount /user/$USER/wordcount/input /user/$USER/wordcount/output
hdfs dfs -cat /user/$USER/wordcount/output/part-r-00000
```

Expected output (ordering can vary):

```text
big       3
data      2
hadoop    2
```

**Remember:** Mapper emits `(word, 1)`; Reducer sums counts. The output directory must not exist before the job runs.

---

## 3. Most Frequent Words

**Aim:** Sort Word Count output by frequency and display the most frequent word. First finish Experiment 2.

```bash
hdfs dfs -cat /user/$USER/wordcount/output/part-r-00000 | sort -k2,2nr
```

Display only the top row:

```bash
hdfs dfs -cat /user/$USER/wordcount/output/part-r-00000 | sort -k2,2nr | head -1
```

Save sorted results locally:

```bash
hdfs dfs -cat /user/$USER/wordcount/output/part-r-00000 | sort -k2,2nr > frequent_words.txt
cat frequent_words.txt
```

Expected most frequent word for the sample input: `big 3`.

> This is the simple lab approach using Linux sort on the MapReduce output. If your examiner specifically requires a separate second MapReduce job, use a second job for global aggregation/sorting.

---

## 4. Hive Data Processing

**Aim:** Create a table with `data`, `user_id`, and `properties`; calculate min/max/distinct age and count records by state.

Start Hive in the terminal with `hive`, then run:

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

Expected min age = 21, max age = 30, distinct ages = 3. State counts: CA = 2, NY = 1, TX = 1.

Exit Hive with:

```sql
QUIT;
```

### Load data from a local file (alternative)

Create a tab-separated text file with exactly three columns matching the table schema, then in Hive:

```sql
LOAD DATA LOCAL INPATH '/absolute/path/user_data.txt' INTO TABLE user_data;
```

Replace the path with the actual file path. Do not run `LOAD DATA` after the same records were already inserted unless duplicate rows are acceptable.

---

## 5. Pig Log Processing

**Aim:** Calculate page views by day, by hour across all dates, and by site. Input columns: `site, hour_of_day, page_views, date`.

### Create sample input and upload

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
hdfs dfs -mkdir -p /user/$USER/data/pig/simplelogs
hdfs dfs -put -f logs.txt /user/$USER/data/pig/simplelogs/
```

### Save as `logs.pig`

```pig
logs = LOAD '/user/YOUR_USERNAME/data/pig/simplelogs/logs.txt'
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

**Important:** Replace `YOUR_USERNAME` with the Linux/HDFS username (or replace the path with the exact HDFS path shown by `hdfs dfs -ls /user/$USER/data/pig/simplelogs`).

Run:

```bash
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

**Aim:** Import a MySQL table to HDFS/Hive and export HDFS records back to MySQL.

> Sqoop is deprecated in many modern distributions. Use these commands only if Sqoop and the MySQL JDBC driver are installed and configured in your college lab.

### A. Create MySQL database and source table

Open MySQL:

```bash
mysql -u root -p
```

Run SQL:

```sql
CREATE DATABASE IF NOT EXISTS bdctdb;
USE bdctdb;
CREATE TABLE students (id INT, name VARCHAR(50), marks INT);
INSERT INTO students VALUES (1,'Aditya',85),(2,'Rahul',90),(3,'Priya',88);
SELECT * FROM students;
EXIT;
```

### B. Import table into HDFS

Run in the Linux terminal. Enter the MySQL password when prompted:

```bash
sqoop import \
  --connect jdbc:mysql://localhost:3306/bdctdb \
  --username root -P \
  --table students \
  --target-dir /user/$USER/sqoop/students \
  --m 1

hdfs dfs -ls /user/$USER/sqoop/students
hdfs dfs -cat /user/$USER/sqoop/students/part-m-00000
```

### C. Import directly into Hive

```bash
sqoop import \
  --connect jdbc:mysql://localhost:3306/bdctdb \
  --username root -P \
  --table students \
  --hive-import \
  --hive-database bdct \
  --hive-table students_sqoop \
  --create-hive-table \
  --m 1
```

Verify:

```bash
hive -e "SELECT * FROM bdct.students_sqoop;"
```

If the target table already exists, use a different table name or configure the import for that existing table.

### D. Export HDFS data to MySQL

Create the destination table in MySQL:

```sql
USE bdctdb;
CREATE TABLE students_export (id INT, name VARCHAR(50), marks INT);
```

Prepare tab-separated records in Linux and upload:

```bash
printf '4\tKiran\t80\n5\tSneha\t92\n' > export.txt
hdfs dfs -mkdir -p /user/$USER/sqoop/export
hdfs dfs -put -f export.txt /user/$USER/sqoop/export/
```

Export:

```bash
sqoop export \
  --connect jdbc:mysql://localhost:3306/bdctdb \
  --username root -P \
  --table students_export \
  --export-dir /user/$USER/sqoop/export \
  --input-fields-terminated-by '\t' \
  --m 1
```

Verify in MySQL:

```sql
SELECT * FROM students_export;
```

---

## Last-minute revision

| Experiment | Memorize |
|---|---|
| HDFS | `-mkdir`, `-put`, `-ls`, `-cat`, `-get` |
| Word Count | Mapper emits `(word,1)`; Reducer sums values |
| Frequent Words | Sort count column descending |
| Hive | `CREATE TABLE`, `REGEXP_EXTRACT`, `MIN`, `MAX`, `COUNT(DISTINCT)`, `GROUP BY` |
| Pig | `LOAD`, `GROUP`, `FOREACH`, `SUM`, `DUMP` |
| Sqoop | `sqoop import`, `--hive-import`, `sqoop export` |

## Environment checks

```bash
hadoop version
hdfs dfs -ls /
java -version
hive --version
pig -version
sqoop version
```
