# Assignment #3: Report

**Name: Lauren Sharpe**
**Student ID: 801371099**
**Email: lsharp22@charlotte.edu**

---

## Seed and commands

Seed used for `datagen.py`:

The commands you ran, in order:

```bash
python3 datagen.py 801371099
docker compose up -d
docker cp main.py spark-master:/opt/spark/work-dir/
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/main.py /opt/spark/work-dir/shared/input /opt/spark/work-dir/shared/output
docker cp main.py spark-master:/opt/spark/work-dir/
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/main.py /opt/spark/work-dir/shared/input /opt/spark/work-dir/shared/output
```

---

## Part 1: load and clean

How you implemented rule 1 (dropoff after pickup) and rule 2 (fare present and numeric):



Why `fare` is read as a string, and what would have happened with a `DOUBLE` column:



The five counts:

| rows_total | bad_time | bad_fare | removed | rows_kept |
| ---------- | -------- | -------- | ------- | --------- |
|   100000   |   1461   |   1513   |  2974   |   97026   |

Two or three bad rows, as they appear in `trips.csv`:

```
T000057,D183,9,1,2026-09-29 15:53:34,2026-09-29 15:44:33,2.67,10.91,card
T000077,D060,4,1,2026-09-18 01:47:52,2026-09-18 02:36:32,14.88,"12,40",cash
T000468,D092,4,9,2026-09-11 08:10:03,2026-09-11 08:31:22,4.95,,card
```

---

## Part 2a: trips and revenue per zone per day

First ten rows, and one sentence on what they show:

```
+----------+-------+---------------+-----+-------+
|day       |zone_id|zone_name      |trips|revenue|
+----------+-------+---------------+-----+-------+
|2026-09-01|1      |Uptown Center  |293  |5749.66|
|2026-09-01|2      |First Ward     |228  |4505.68|
|2026-09-01|3      |Fourth Ward    |188  |3610.21|
|2026-09-01|4      |South End      |262  |5206.28|
|2026-09-01|5      |Dilworth       |201  |4032.84|
|2026-09-01|6      |Plaza Midwood  |134  |2754.68|
|2026-09-01|7      |NoDa           |193  |3907.03|
|2026-09-01|8      |University City|220  |4256.82|
|2026-09-01|9      |Ballantyne     |164  |3384.95|
|2026-09-01|10     |SouthPark      |167  |3393.51|git
+----------+-------+---------------+-----+-------+
only showing top 20 rows
```

## Part 2b: each driver's busiest day

First ten rows, and one sentence on what they show:

```

```

## Part 3: activity per borough (Spark SQL)

Your SQL query and its result:

```sql

```

```

```

---

## The two plans

`explain()` output of part 2a (DataFrame API):

```

```

`explain()` output of part 3 (Spark SQL):

```

```

Your comparison: where the file scans are, which operator is the join and of which kind,
where the exchanges are and why, and what the two plans have in common although they were
written in different languages.



---

## At scale

With 200 million trips instead of 100,000: what would you keep as it is, and what would you
change first? One paragraph.



---

## Problems and fixes

What went wrong and how you fixed it, with the actual error messages. If nothing went wrong,
say so.

[PARSE_SYNTAX_ERROR] Syntax error at or near 'ad'. SQLSTATE: 42601 (line 1, pos 14)

== SQL ==
try_cast(fare ad double)

mispelled fare as double which I just corrected by fixing the spelling. 
