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
docker cp main.py spark-master:/opt/spark/work-dir/
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/main.py /opt/spark/work-dir/shared/input /opt/spark/work-dir/shared/output
docker cp main.py spark-master:/opt/spark/work-dir/
docker exec -it spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/main.py /opt/spark/work-dir/shared/input /opt/spark/work-dir/shared/output
```

---

## Part 1: load and clean

How you implemented rule 1 (dropoff after pickup) and rule 2 (fare present and numeric):
To implement the rules I first converted the original fare string to a double using try_cast, which would return null for any missing or non numeric faress for rule 2. Then I checked if a trip had a valid time by seeing if the dropoff was less than or equal to its pickup. I then counted these messed up rows and removed any row that violated one of the rules. 



Why `fare` is read as a string, and what would have happened with a `DOUBLE` column:
It helps prevent losing any malformed values before proper inspection and conversion. If you used double, invalid fare values would become null during CSV loading, so you wouldn't be able to tell the difference between a missing fare and a malformed fare. 



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
|2026-09-01|10     |SouthPark      |167  |3393.51|
+----------+-------+---------------+-----+-------+
This shows the revenue and trips per zone on a particular day and we can see that Uptown Center had the most trips and the most revenue on September 1st 2026.
```

## Part 2b: each driver's busiest day

First ten rows, and one sentence on what they show:

```
+---------+----------+-----+
|driver_id|day       |trips|
+---------+----------+-----+
|D001     |2026-09-23|23   |
|D002     |2026-09-21|20   |
|D003     |2026-09-08|18   |
|D004     |2026-09-12|26   |
|D005     |2026-09-11|22   |
|D006     |2026-09-03|19   |
|D007     |2026-09-03|21   |
|D008     |2026-09-07|19   |
|D009     |2026-09-10|19   |
|D010     |2026-09-03|21   |
+---------+----------+-----+
```
This is showing the drivers and what day they performed the most trips. We can see that DOO1's busiest day was on September 23rd 2026 and they had 23 trips. 

## Part 3: activity per borough (Spark SQL)

Your SQL query and its result:

```sql
        SELECT 
            z.borough,
            COUNT(*) AS trips,
            ROUND(SUM(t.fare), 2) AS revenue,
            ROUND(AVG(t.distance_km), 2) AS avg_distance_km
        FROM trips t
        JOIN zones z ON t.pickup_zone = z.zone_id
        GROUP BY z.borough
        ORDER BY revenue DESC

```

```
+-------+-----+---------+---------------+
|borough|trips|revenue  |avg_distance_km|
+-------+-----+---------+---------------+
|Central|37104|732150.57|5.35           |
|South  |17940|355932.75|5.38           |
|North  |15900|314264.45|5.35           |
|East   |13764|273587.22|5.39           |
|West   |12318|243523.53|5.36           |
+-------+-----+---------+---------------+

```

---

## The two plans

`explain()` output of part 2a (DataFrame API):

```
AdaptiveSparkPlan isFinalPlan=false
+- Sort [day#39 ASC NULLS FIRST, zone_id#9 ASC NULLS FIRST], true, 0
   +- Exchange rangepartitioning(day#39 ASC NULLS FIRST, zone_id#9 ASC NULLS FIRST, 200), ENSURE_REQUIREMENTS, [plan_id=1474]
      +- Project [day#39, zone_id#9, zone_name#10, trips#100L, revenue#101]
         +- BroadcastHashJoin [pickup_zone#2], [zone_id#9], Inner, BuildRight, false, false
            :- HashAggregate(keys=[day#39, pickup_zone#2], functions=[count(1), sum(fare#38)])
            :  +- Exchange hashpartitioning(day#39, pickup_zone#2, 200), ENSURE_REQUIREMENTS, [plan_id=1467]
            :     +- HashAggregate(keys=[day#39, pickup_zone#2], functions=[partial_count(1), partial_sum(fare#38)])
            :        +- Project [pickup_zone#2, try_cast(fare#7 as double) AS fare#38, cast(pickup_ts#4 as date) AS day#39]
            :           +- Filter (((((isnotnull(dropoff_ts#5) AND isnotnull(pickup_ts#4)) AND isnotnull(fare#7)) AND (dropoff_ts#5 > pickup_ts#4)) AND isnotnull(try_cast(fare#7 as double))) AND isnotnull(pickup_zone#2))
            :              +- FileScan csv [pickup_zone#2,pickup_ts#4,dropoff_ts#5,fare#7] Batched: false, DataFilters: [isnotnull(dropoff_ts#5), isnotnull(pickup_ts#4), isnotnull(fare#7), (dropoff_ts#5 > pickup_ts#4)..., Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/trips.csv], PartitionFilters: [], PushedFilters: [IsNotNull(dropoff_ts), IsNotNull(pickup_ts), IsNotNull(fare), IsNotNull(pickup_zone)], ReadSchema: struct<pickup_zone:int,pickup_ts:timestamp,dropoff_ts:timestamp,fare:string>
            +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1470]
               +- Filter isnotnull(zone_id#9)
                  +- FileScan csv [zone_id#9,zone_name#10] Batched: false, DataFilters: [isnotnull(zone_id#9)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/zones.csv], PartitionFilters: [], PushedFilters: [IsNotNull(zone_id)], ReadSchema: struct<zone_id:int,zone_name:string>
```

`explain()` output of part 3 (Spark SQL):

```
+- Sort [revenue#190 DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(revenue#190 DESC NULLS LAST, 200), ENSURE_REQUIREMENTS, [plan_id=1524]
      +- HashAggregate(keys=[borough#11], functions=[count(1), sum(fare#38), avg(distance_km#6)])
         +- Exchange hashpartitioning(borough#11, 200), ENSURE_REQUIREMENTS, [plan_id=1521]
            +- HashAggregate(keys=[borough#11], functions=[partial_count(1), partial_sum(fare#38), partial_avg(distance_km#6)])
               +- Project [distance_km#6, fare#38, borough#11]
                  +- BroadcastHashJoin [pickup_zone#2], [zone_id#9], Inner, BuildRight, false, false
                     :- Project [pickup_zone#2, distance_km#6, try_cast(fare#7 as double) AS fare#38]
                     :  +- Filter (((((isnotnull(dropoff_ts#5) AND isnotnull(pickup_ts#4)) AND isnotnull(fare#7)) AND (dropoff_ts#5 > pickup_ts#4)) AND isnotnull(try_cast(fare#7 as double))) AND isnotnull(pickup_zone#2))
                     :     +- FileScan csv [pickup_zone#2,pickup_ts#4,dropoff_ts#5,distance_km#6,fare#7] Batched: false, DataFilters: [isnotnull(dropoff_ts#5), isnotnull(pickup_ts#4), isnotnull(fare#7), (dropoff_ts#5 > pickup_ts#4)..., Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/trips.csv], PartitionFilters: [], PushedFilters: [IsNotNull(dropoff_ts), IsNotNull(pickup_ts), IsNotNull(fare), IsNotNull(pickup_zone)], ReadSchema: struct<pickup_zone:int,pickup_ts:timestamp,dropoff_ts:timestamp,distance_km:double,fare:string>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)),false), [plan_id=1516]
                        +- Filter isnotnull(zone_id#9)
                           +- FileScan csv [zone_id#9,borough#11] Batched: false, DataFilters: [isnotnull(zone_id#9)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/opt/spark/work-dir/shared/input/zones.csv], PartitionFilters: [], PushedFilters: [IsNotNull(zone_id)], ReadSchema: struct<zone_id:int,borough:string>
```

Your comparison: where the file scans are, which operator is the join and of which kind,
where the exchanges are and why, and what the two plans have in common although they were
written in different languages.

Both plans begin with Filescan csv operators that read trips.csv and zones.csv. Spark reads only the columns needed for each query though. For example in part 3 it needs to read distance.km but part 2a doesn't need to read it since it doesn't use that column. Both plans filter out trips whose dropoff is not after pickup and fares that can't be converted to numebers. Both queries also use an inner BroadcastHashJoin on pickup_zone = zone_id. Although they were written in different languanges they are both translated into Spark plans and use similar operations.

---

## At scale

With 200 million trips instead of 100,000: what would you keep as it is, and what would you
change first? One paragraph.

I would keep the explicit schema, safe fare conversion, filtering rules, and using Datafram and SQL operations. Instead of using CSV files I would replace them with Parquet files so Spark can read them more efficiently. I would also replace the outputs so instead of using a single file I would allow distrubted writes. I would also want to inspect exeecution plans and shuffles sizes to tune partion counts. 

---

## Problems and fixes

What went wrong and how you fixed it, with the actual error messages. If nothing went wrong,
say so.

[PARSE_SYNTAX_ERROR] Syntax error at or near 'ad'. SQLSTATE: 42601 (line 1, pos 14)

== SQL ==
try_cast(fare ad double)

mispelled fare as double which I just corrected by fixing the spelling. 
[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name revenue cannot be resolved. Did you mean one of the following? [day, trips, borough, zone_id, zone_name].

Accidently put the alias inside the sround method. To fix it I corrected the parrenthesis order.