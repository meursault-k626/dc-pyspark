**1.** Which of the following is NOT one of the classic "3 V's" of Big Data covered in the notes?

- [ ] A) Volume
- [ ] B) Variety
- [ ] C) Velocity
- [ ] D) Veracity

---

**2.** Fill in the blanks to match each computing concept to its definition:

1. Pooling the resources of multiple machines to complete jobs: `______` computing
2. Carrying out many calculations simultaneously: `______` computing
3. Networked nodes running jobs in parallel: `______` computing
4. Breaking data into smaller pieces and running each piece on an individual machine (results in chunks): `______` processing

---

**3.** How much faster is Apache Spark compared to Hadoop MapReduce according to the performance benchmarks in the notes?

- [ ] A) Up to 10× faster in memory, and up to 2× faster on disk
- [ ] B) Up to 100× faster in memory, and up to 10× faster on disk
- [ ] C) Up to 1000× faster in memory, and up to 100× faster on disk
- [ ] D) Up to 50× faster in memory, and equal speed on disk

---

**4.** Fill in the blanks regarding Apache Spark's architecture and ecosystem:

1. Spark is written mainly in `______`, with APIs also available in Java, Python, R, and SQL.
2. The component at the center of the ecosystem containing basic functionality on which everything else sits is `______`.
3. The four libraries built on top of the core are Spark SQL, `______` (machine learning), `______` (graphs), and Spark `______` (real-time data).

---

**5.** What code change is required when transitioning a PySpark script from local mode (development/testing on a laptop) to cluster mode (production)?

- [ ] A) All DataFrame transformations must be rewritten as RDD transformations.
- [ ] B) No code change is necessary; only the `master` URL configuration changes.
- [ ] C) Every `spark.read` call must be wrapped in `sc.parallelize()`.
- [ ] D) `SparkSession` must be replaced with `ClusterSession`.

---

**6.** Fill in the blanks with the exact command or attribute name:

1. Spark ships with three interactive shells: `spark-shell` (Scala), `______` (Python), and `______` (R).
2. Given a `SparkContext` exposed as `sc`, inspect the Spark version using `sc.______`, the Python version using `sc.______`, and the cluster URL / local string using `sc.______`.

---

**7.** Which statement accurately describes a Python `lambda` function and how it interacts with `map()` and `filter()`?

- [ ] A) A `lambda` body is one expression returned implicitly; `map()` transforms every item (same output length) while `filter()` keeps only truthy items.
- [ ] B) A `lambda` can span several lines as long as the body ends with an explicit `return`, which is how multi-statement transformations get passed to `map()` without a named function.
- [ ] C) `map(f, lst)` expects `f` to be a predicate returning a boolean, whereas `filter(f, lst)` expects a transformer; both return an output list the same length as the input.
- [ ] D) `if` / `for` / `while` blocks are allowed inside a `lambda` body, but ternary expressions such as `A if cond else B` are rejected because they evaluate lazily.

---

**8.** Fill in the blanks to complete the plain Python functional programming expressions:

```python
my_list = [1, 2, 3, 4, 5, 6, 7, 8, 9]
# Square all numbers in my_list
squared_list = list(______(lambda x: x ** 2, my_list))

my_list2 = [10, 21, 31, 40, 51, 60, 72, 80, 93, 101]
# Keep only numbers divisible by 10
filtered_list = list(______(lambda x: (x % 10 == 0), my_list2))
```

---

**9.** What do the three words in **RDD** (Resilient Distributed Datasets) specifically mean?

- [ ] A) **Relational** — stored in SQL table format with enforced column types; **Dynamic** — mutable in place so partitions can be edited; **Datasets** — unpartitioned files kept on the driver.
- [ ] B) **Redundant** — every partition is replicated ten times across disk; **Decentralized** — runs with no driver node at all; **DataFrames** — always carries named, typed columns.
- [ ] C) **Resilient** — recomputes missing or damaged partitions; **Distributed** — spans jobs across cluster nodes; **Datasets** — a collection of partitioned data.
- [ ] D) **Rapid** — held in memory only and never spilled to disk; **Deterministic** — evaluated eagerly in code order; **Dictionaries** — restricted to key-value pairs exclusively.

---

**10.** Fill in the blanks to create RDDs and inspect their partitioning:

```python
# 1. Create an RDD from a Python list with a minimum of 6 partitions
numRDD = sc.______([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], ______=6)

# 2. Create an RDD from a local text file "README.md" with a minimum of 5 partitions
fileRDD = sc.______("README.md", ______=5)

# 3. Print the number of partitions in fileRDD
print(fileRDD.______())
```

---

**11.** Given `rdd = sc.parallelize(["hello world", "how are you"])` (which has 2 elements), what is the difference between `rdd.map(lambda x: x.split(" "))` and `rdd.flatMap(lambda x: x.split(" "))`?

- [ ] A) `map` returns 2 list elements, `[['hello','world'], ['how','are','you']]`; `flatMap` flattens them into 5 string elements.
- [ ] B) `map` returns 5 string elements because `split()` is applied per word, while `flatMap` returns 2 list elements by regrouping those words back into their original rows.
- [ ] C) There is no difference here — both return 5 elements, because Spark always flattens the output of a string `split()` before it builds the resulting RDD partitions.
- [ ] D) `map` is a lazy transformation that only extends the DAG, whereas `flatMap` is an eager action that triggers computation and returns a Python list to the driver.

---

**12.** Fill in the blanks with the appropriate RDD transformation or action:

```python
inputRDD = sc.textFile("logs.txt")

# 1. Keep only lines containing "error" or "warning"
errorsRDD = inputRDD.______(lambda line: "error" in line.split())
warningsRDD = inputRDD.______(lambda line: "warning" in line.split())

# 2. Combine both RDDs into a single RDD
combinedRDD = errorsRDD.______(warningsRDD)

# 3. Return the first element as a single value (not a list)
first_log = combinedRDD.______()

# 4. Return the first 4 elements as a Python list
top_four = combinedRDD.______(4)

# 5. Return the total number of elements in the RDD
total_logs = combinedRDD.______()
```

---

**13.** Why is `reduceByKey()` generally preferred over `groupByKey()` when aggregating large pair RDDs?

- [ ] A) `groupByKey()` is an action that pulls the entire dataset back to the driver, whereas `reduceByKey()` is a lazy transformation returning a new RDD of reduced values.
- [ ] B) `groupByKey()` accepts only integer keys and silently drops string keys, while `reduceByKey()` handles any key type for which an ordering has been defined.
- [ ] C) `reduceByKey()` combines values locally on each partition before shuffling, whereas `groupByKey()` shuffles every key-value pair across the network.
- [ ] D) `reduceByKey()` automatically sorts its output by key in descending order, removing the need for a separate `sortByKey()` call later in the pipeline.

---

**14.** Fill in the blanks to complete the Shakespeare word-count pipeline using Pair RDD transformations:

```python
baseRDD = sc.textFile("Complete_Shakespeare.txt")
splitRDD = baseRDD.______(lambda x: x.split())
splitRDD_no_stop = splitRDD.______(lambda x: x.lower() not in stop_words)

# Emit (word, 1) tuples and sum counts per word
resultRDD = (
    splitRDD_no_stop
    .______(lambda w: (w, 1))
    .______(lambda x, y: x + y)
)

# Swap (word, count) -> (count, word) and sort by count in descending order
resultRDD_swap_sort = (
    resultRDD
    .map(lambda x: (x[1], x[0]))
    .______(ascending=______)
)
```

---

**15.** You have two pair RDDs keyed by player name:
`RDD1 = sc.parallelize([("L. Messi", 30), ("Neymar", 25)])`
`RDD2 = sc.parallelize([("L. Messi", "€565K"), ("Neymar", "€280K")])`
What does `RDD1.join(RDD2).collect()` return?

- [ ] A) `[('L. Messi', 30, '€565K'), ('Neymar', 25, '€280K')]`
- [ ] B) `[('L. Messi', (30, '€565K')), ('Neymar', (25, '€280K'))]`
- [ ] C) `[('L. Messi', 30), ('Neymar', 25), ('L. Messi', '€565K'), ('Neymar', '€280K')]`
- [ ] D) `{'L. Messi': (30, '€565K'), 'Neymar': (25, '€280K')}`

---

**16.** Fill in the blanks to filter with the DataFrame API, register the intermediate result as its own temporary view, aggregate that view with SQL, and chain a DataFrame operation onto the result:

```python
# 1. Filter for salaries over 3000 using the DataFrame API
filtered_df = df.______(df.Salary > 3000)

# 2. Register the FILTERED DataFrame as its own temporary view
filtered_df.______("filtered_employees")

# 3. Aggregate that view with SQL - count the employees per department
result_df = spark.______("""
    SELECT Department, ______(*) AS Employee_Count
    FROM filtered_employees
    ______ Department
""")

# 4. The result is itself a DataFrame, so DataFrame operations still chain onto it
result_df.______("Employee_Count", ascending=False).show()
```

---

**17.** Which of the following correctly contrasts `SparkContext` (`sc`) and `SparkSession` (`spark`)?

- [ ] A) `SparkContext` is the entry point for RDDs; `SparkSession` is the unified entry point for DataFrames, temp views and SQL.
- [ ] B) `SparkSession` is the low-level entry point used to create RDDs, while `SparkContext` is the higher-level handle for DataFrames, temp views and SQL queries.
- [ ] C) `SparkSession.builder.getOrCreate()` always terminates any existing session first and then starts a fresh duplicate, which is why it is safe to call it repeatedly.
- [ ] D) `SparkContext` is reachable only from Scala and Java, whereas `SparkSession` was introduced specifically to serve the Python and R APIs.

---

**18.** Fill in the blanks to create a `SparkSession`, build a DataFrame from an RDD, and read a CSV file:

```python
from pyspark.sql import ______

# 1. Create or retrieve a SparkSession named "my_spark"
spark = SparkSession.______.______("my_spark").______()

# 2. Create a DataFrame from an existing RDD with column names ['Name', 'Age']
names_df = spark.______(rdd, schema=['Name', 'Age'])

# 3. Read "people.csv" using the first row as column names and automatically inferring data types
people_df = spark.read.csv("people.csv", ______=True, ______=True)
```

---

**19.** A CSV file `adult_reduced.csv` has columns named `education.num` and `marital.status`. Why does `df.select("education.num").show()` fail in PySpark, and how do you fix it inside `select()`?

- [ ] A) Column names are capped at ten characters by Spark's CSV reader, so the name has to be truncated to `"education"` or renamed on load with `.toDF()`.
- [ ] B) Spark reads the dot as nested struct field access; wrap the name in backticks — ``df.select("`education.num`")``.
- [ ] C) Dots are reserved for referencing SQL temporary views, so the column must be wrapped in square brackets instead — `df.select("[education.num]")`.
- [ ] D) `select()` accepts only `Column` objects and never plain strings, so the fix is `df.select(df["education.num"])` with the column looked up by index.

---

**20.** Fill in the blanks with the appropriate DataFrame methods/attributes to inspect and transform `people_df`:

```python
# 1. Print the schema in a tree format
people_df.______()

# 2. Access the list of column names (note: attribute, not a method!)
col_names = people_df.______

# 3. Compute summary statistics (count, mean, stddev, min, max) for numerical columns
people_df.______().show()

# 4. Remove duplicate rows judged only across 'User_ID', 'Age', and 'Gender'
no_dup_df = test_df.______(['User_ID', 'Age', 'Gender'])

# 5. Rename column 'Gender' to 'Sex'
sex_df = test_df.______('Gender', 'Sex')
```

---

**21.** Which of the following DataFrame operations are **lazy transformations** rather than eager actions?

* **I.** `dropDuplicates()`
* **II.** `count()`
* **III.** `withColumnRenamed()`
* **IV.** `collect()`

- [ ] A) I and III only
- [ ] B) II and IV only
- [ ] C) I, II and III
- [ ] D) III and IV only

---

**22.** Fill in the blanks to filter, group, aggregate, and sort a DataFrame in descending order:

```python
from pyspark.sql.functions import avg, desc

# 1. Filter for company_location == "CA" and experience_level == "EN", then calculate average salary_in_usd across all matching rows
ca_entry_avg = (
    ca_salaries_df
    .______(ca_salaries_df['company_location'] == "CA")
    .______(ca_salaries_df['experience_level'] == "EN")
    .groupBy()
    .______("salary_in_usd")
)

# 2. Group fifa21 by 'Club', compute average 'Overall', rename to 'avg_overall', and sort descending
club_ovr = (
    fifa21
    .______('Club')
    .avg('Overall')
    .withColumnRenamed('avg(Overall)', 'avg_overall')
    .______(______('avg_overall'))
)
```

---

**23.** Compare CSV, JSON, and Parquet formats in PySpark. Which statement is FALSE?

- [ ] A) CSV has no schema enforcement, cannot be shared across workers during import of a single file, and does not support predicate pushdown.
- [ ] B) JSON naturally supports nested/hierarchical data and arrays, but is storage-heavy at scale due to repeated keys.
- [ ] C) Parquet is a compressed, columnar binary format with an embedded schema that supports predicate pushdown and is ideal for intermediate storage.
- [ ] D) Parquet files are plain-text files that can be opened and edited directly in Notepad, just like CSVs.

---

**24.** Fill in the blanks to manually define a schema using `pyspark.sql.types` and apply it when reading `adult_reduced.csv`:

```python
from pyspark.sql.types import (
    ______, ______,
    IntegerType, StringType, DoubleType
)

schema = ______([
    ______("age", IntegerType(), True),
    ______("education_num", IntegerType(), True),
    ______("marital_status", StringType(), True),
    ______("occupation", StringType(), True),
    ______("income", DoubleType(), True)
])

df = spark.read.csv("adult_reduced.csv", header=True, ______=schema)
```

---

**25.** Why does defining an explicit schema (`schema=...`) improve CSV import performance compared to using `inferSchema=True`?

- [ ] A) `inferSchema=True` converts the CSV into JSON in a staging directory before loading it, and that extra round-trip is the cost an explicit schema avoids entirely.
- [ ] B) An explicit schema makes Spark compress the source file to `.gz` on disk first, so every subsequent read has far fewer bytes to pull across the network.
- [ ] C) Without a schema Spark reads the data twice — once to infer types, once to load; an explicit schema needs one read and filters bad rows during it.
- [ ] D) `inferSchema=True` disables the worker nodes and forces the driver to parse the whole file single-threaded, whereas a schema lets every worker parse in parallel.

---

**26.** Fill in the blanks to handle missing data and manipulate columns on a DataFrame:

```python
from pyspark.sql.functions import col

# 1. Drop any row containing a null value across any column
df_cleaned1 = df.______.______()

# 2. Drop rows only when ALL values in the row are null
df_cleaned2 = df.na.drop(______="all")

# 3. Filter to keep rows where "columnName" is not null
df_cleaned3 = df.where(col("columnName").______())

# 4. Replace nulls in "income" with 0 and in "occupation" with "Unknown"
df_filled = df.______.______({"income": 0, "occupation": "Unknown"})

# 5. Add a derived column "weekly_salary" (income / 52) and drop the old "income" column
df_transformed = (
    df_filled
    .______("weekly_salary", df_filled["income"] / 52)
    .______("income")
)
```

---

**27.** Which statement is TRUE regarding `union()` and `join()` in PySpark DataFrames?

- [ ] A) `df1.union(df2)` matches columns by name regardless of their order, so only the column count and the data types actually need to line up between the two.
- [ ] B) PySpark's `.join()` supports inner joins only; left, right and full outer joins have to be written as SQL strings and executed through `spark.sql()`.
- [ ] C) When the join columns carry different names in each DataFrame, `.join()` cannot be used at all until one of the two sides has been renamed first.
- [ ] D) `union()` is positional — identical column count, types and order are required; for name-based union use `unionByName()`.

---

**28.** Fill in the blanks to rename a key column and perform a left outer join between `flights` and `airports`:

```python
# Rename "faa" to "dest" on airports
airports = airports.______("faa", "dest")

# Join flights with airports on "dest" using a left outer join
flights_with_airports = flights.______(
    airports,
    ______="dest",
    ______="leftouter"
)
```

---

**29.** What is the key difference between a **Map** column (`create_map`) and a **Struct** column (`struct`), and what happens if you pass an odd number of arguments to `create_map()`?

- [ ] A) A Struct has a fixed, named, typed schema read via dot notation; a Map holds per-row dynamic keys. An odd argument count raises an `AnalysisException`.
- [ ] B) A Struct carries dynamic keys that may vary from row to row while a Map enforces one fixed schema; an odd argument count makes `create_map()` pad the final value with `0`.
- [ ] C) Maps may hold only integer values and Structs only strings, which is why `create_map()` rejects an odd number of arguments rather than coercing the trailing key.
- [ ] D) Both are aliases for the same constructor in `pyspark.sql.functions`, so an odd argument count is accepted and the unmatched key is simply stored against a null.

---

**30.** Fill in the blanks to construct Array, Map, and Struct columns on a DataFrame:

```python
from pyspark.sql.functions import ______, ______, ______, ______

# 1. Create an Array column "skills" containing "Python", "SQL", "Spark"
df = df.withColumn("skills", ______(______("Python"), lit("SQL"), lit("Spark")))

# 2. Create a Map column "attributes" with {"colour": "red", "size": "M"}
df = df.withColumn("attributes", ______(lit("colour"), lit("red"), lit("size"), lit("M")))

# 3. Create a Struct column "address" with fields street, city, postcode
df = df.withColumn(
    "address",
    ______(
        lit("221B Baker Street").alias("street"),
        lit("London").alias("city"),
        lit("NW1").alias("postcode")
    )
)

# 4. Select just the nested 'city' field from 'address' using dot notation
df.select("______").show()
```

---

**31.** Which statement accurately compares standard **PySpark UDFs** (`udf`) and **pandas UDFs** (`@pandas_udf`)?

- [ ] A) Standard PySpark UDFs are vectorised through Apache Arrow and suit massive datasets, whereas pandas UDFs fall back to row-at-a-time evaluation and suit only tiny ones.
- [ ] B) Standard UDFs run row-by-row with JVM↔Python serialisation cost and are opaque to Catalyst; pandas UDFs process batches as vectorised Series.
- [ ] C) Using the `@udf(StringType())` decorator makes a standard UDF roughly ten times faster than the same function wrapped with `F.udf(fn, StringType())` at call time.
- [ ] D) Standard PySpark UDFs infer their return type from the first row they process, whereas pandas UDFs always default to `StringType()` unless cast afterwards.

---

**32.** Fill in the blanks to define and apply a standard PySpark UDF (both function-call and decorator styles) and a pandas UDF:

```python
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, ______
from pyspark.sql.types import StringType, DoubleType

# 1. Function-call form: wrap getFirstAndMiddle to return a StringType
def getFirstAndMiddle(names):
    return ' '.join(names[:-1])

udfFirstAndMiddle = F.______(getFirstAndMiddle, ______())
voter_df = voter_df.withColumn('first_and_middle_name', udfFirstAndMiddle(voter_df.splits))

# 2. Decorator form for a pandas UDF returning DoubleType that adds 10 to a column
@______(DoubleType())
def add_ten_pandas(column):
    return column + 10

df = df.withColumn("10_plus", ______(df["value"]))
```

---

**33.** You want to run a SQL query on a DataFrame `df`. Which of the following statements about temporary views in Spark SQL is TRUE?

- [ ] A) `spark.sql("SELECT * FROM df")` can be run directly against any Python DataFrame variable, because Spark resolves the variable name in the calling scope automatically.
- [ ] B) Running a `SELECT ... WHERE` query against a temporary view rewrites the underlying CSV on disk, which is why registering a view is said to protect the data source.
- [ ] C) `createOrReplaceTempView()` registers a session-scoped view that dies with the session; `createGlobalTempView()` is cross-session, queried via `global_temp.`
- [ ] D) `spark.sql()` returns a raw Python list of tuples rather than a DataFrame, so `.withColumn()` and `.filter()` cannot be chained onto its result without converting it.

---

**34.** Fill in the blanks to cast a string column to an integer, register a temporary view, and run a SQL aggregation query:

```python
# 1. Convert the "Salary" string column to an integer
df = df.withColumn("Salary", df["Salary"].______("int"))

# 2. Filter salaries over 3000 and register as a session-scoped temporary view "filtered_employees"
filtered_df = df.filter(df.Salary > 3000)
filtered_df.______("filtered_employees")

# 3. Execute a SQL query to count employees per Department
result_df = spark.______("""
    SELECT Department, COUNT(*) AS Employee_Count
    FROM filtered_employees
    GROUP BY Department
""")
result_df.show()
```

---

**35.** When optimizing PySpark jobs at scale, what do `.explain()` and `broadcast()` do?

- [ ] A) `.explain()` prints summary statistics such as mean and standard deviation for every numerical column on the driver, while `broadcast()` writes the DataFrame out to object storage.
- [ ] B) `.explain()` rewrites slow Python UDFs into equivalent Scala so that Catalyst can optimise them, while `broadcast()` splits one large CSV into many smaller chunks before import.
- [ ] C) `.explain()` is an action that caches the DataFrame to local disk as a side effect, while `broadcast()` releases cached tables to free memory ahead of the upcoming join.
- [ ] D) `.explain()` prints the logical and physical plans so redundant shuffles become visible; `broadcast(small_df)` sends a small table to every worker.

---

**36.** Fill in the blanks to cache, persist with a specific storage level, inspect cache status, and release a DataFrame from cache:

```python
from pyspark import ______

# 1. Mark departures_df for default in-memory caching and trigger it with count()
departures_df = departures_df.distinct().______()
departures_df.count()

# 2. Check boolean property indicating whether departures_df is currently cached (no parentheses!)
print(departures_df.______)

# 3. Remove departures_df from the cache to free memory
departures_df.______()

# 4. Persist salaries_df to memory with disk fallback if RAM is insufficient
salaries_df.______(______.MEMORY_AND_DISK)
```

---

**37.** Why are Spark DataFrames **immutable**, and how does **lazy processing** work alongside immutability?

- [ ] A) Immutability lets Spark share data safely across concurrent components with no locks; each transformation builds a new recipe, and nothing runs until an action.
- [ ] B) Immutability means that once a CSV has been read by Spark, the file on disk is locked read-only for the lifetime of the SparkSession to prevent concurrent writes.
- [ ] C) Lazy processing means Spark deliberately waits a fixed interval before executing each line, batching up nearby statements so they can be sent to the cluster together.
- [ ] D) Immutability applies only to RDDs; DataFrames are mutated in place exactly as pandas DataFrames are, which is what makes the DataFrame API the faster of the two.

---

**38.** Fill in the blanks to union two departure DataFrames, rename a column, save as Parquet (overwriting any existing file), and query the Parquet data via SQL:

```python
df1 = spark.read.format("csv").load("AA_DFW_2017_Departures_Short.csv.gz")
df2 = spark.read.format("csv").load("AA_DFW_2016_Departures_Short.csv.gz")

# 1. Combine df1 and df2 vertically
df3 = df1.______(df2)
df3_renamed = df3.withColumnRenamed("_c3", "flight_duration")

# 2. Write df3_renamed to 'AA_DFW_ALL.parquet', overwriting if it exists
df3_renamed.write.______('AA_DFW_ALL.parquet', ______='overwrite')

# 3. Read the Parquet file back and register as temp view 'flights'
flights_df = spark.______.______('AA_DFW_ALL.parquet')
flights_df.createOrReplaceTempView('flights')
```

---

**39.** What is **predicate pushdown**, and why is it a major advantage of Parquet over CSV?

- [ ] A) It pushes the Python `lambda` functions supplied to `filter()` down onto the driver's GPU, where predicate evaluation runs far faster than it does on the worker CPUs.
- [ ] B) It filters during the file-read stage itself, so irrelevant chunks never leave disk; CSV cannot use it because the whole text file must be parsed first.
- [ ] C) It automatically sorts all rows alphabetically as a file is written, so that later range queries can stop reading as soon as the first non-matching row is encountered.
- [ ] D) It converts SQL `GROUP BY` clauses into equivalent `JOIN` clauses, which Catalyst is able to reorder more aggressively than it can reorder grouped aggregations.

---

**40.** Which of the following live in `pyspark.sql.functions` (the `F.*` family), rather than being called as a method on a Column?

* **I.** `split()`
* **II.** `cast()`
* **III.** `size()`
* **IV.** `getItem()`

- [ ] A) I, II and III
- [ ] B) II and IV only
- [ ] C) I and III only
- [ ] D) I, III and IV

---

**41.** Fill in the blanks to clean `voter_df` (`DallasCouncilVoters.csv.gz`) using string functions and `ArrayType` operations:

```python
import pyspark.sql.functions as F

# 1. Filter voter_df where VOTER_NAME length is > 0 and < 20
voter_df = voter_df.filter('______(VOTER_NAME) > 0 and ______(VOTER_NAME) < 20')

# 2. Filter out rows where VOTER_NAME contains an underscore "_"
voter_df = voter_df.filter(______ F.col('VOTER_NAME').______("_"))

# 3. Split VOTER_NAME on whitespace into an ArrayType column 'splits'
voter_df = voter_df.withColumn("splits", F.______(voter_df.VOTER_NAME, r'\s+'))

# 4. Extract the first item (index 0) as 'first_name' and the last item as 'last_name'
voter_df = voter_df.withColumn("first_name", voter_df.splits.______(0))
voter_df = voter_df.withColumn("last_name", voter_df.splits.______(F.______('splits') - 1))
```

---

**42.** You use `F.when()` and `.otherwise()` to create a conditional column:
```python
df.select(
    df.Name,
    df.Age,
    F.when(df.Age >= 18, "Adult")
)
```
What happens for rows where `df.Age < 18`, and how many `.when()` and `.otherwise()` clauses can be chained in a single expression?

- [ ] A) Rows with `Age < 18` receive the string `"False"`; at most two `.when()` clauses and two `.otherwise()` clauses may be chained together in a single column expression.
- [ ] B) Spark raises a `NullPointerException` as soon as an unmatched row is reached, which is why `.otherwise()` is mandatory whenever the conditions do not cover every row.
- [ ] C) `.otherwise()` has to be called before the first `.when()`, because it establishes the default that each subsequent condition then selectively overrides.
- [ ] D) Rows with `Age < 18` get `null`; any number of `.when()` clauses can be chained (first match wins) but at most one `.otherwise()` at the end.

---

**43.** Fill in the blanks to assign `random_val` using `F.when()` and `.otherwise()`: assign `F.rand()` if `TITLE` is `'Councilmember'`, `2` if `TITLE` is `'Mayor'`, and `0` otherwise:

```python
voter_df = voter_df.withColumn(
    'random_val',
    F.______(voter_df.TITLE == 'Councilmember', F.rand())
     .______(voter_df.TITLE == 'Mayor', 2)
     .______(0)
)
```

---

**44.** The final step of a cleaning pipeline computes derived values from existing columns. Why do the notes recommend doing these calculations **inline** rather than inside a UDF wherever possible?

- [ ] A) Spark can optimise inline column maths, whereas a UDF carries a real performance penalty; UDFs stay the fallback for logic the built-ins cannot express.
- [ ] B) Inline calculations are evaluated on the driver rather than on the workers, which removes the network round trip that a distributed UDF would otherwise have to pay.
- [ ] C) A UDF cannot return a floating-point value, so any average or ratio has to be written as inline column arithmetic to stop the result being truncated to an integer.
- [ ] D) Inline maths is applied once per partition rather than once per row, so the result is identical but the number of function invocations drops by orders of magnitude.

---

**45.** Fill in the blanks to repartition a DataFrame, add monotonic IDs, and continue ID numbering from a previous DataFrame's maximum ID:

```python
# 1. Repartition voter_df into a single partition
voter_df_single = voter_df.select(F.col("VOTER NAME")).______(1)

# 2. Determine the highest ROW_ID in voter_df_march
previous_max_ID = voter_df_march.select('ROW_ID').rdd.______()[0]

# 3. Add a ROW_ID column to voter_df_april starting at previous_max_ID + 1
voter_df_april = voter_df_april.withColumn(
    'ROW_ID',
    F.______() + previous_max_ID + 1
)
```

---

**46.** Which of the following is TRUE regarding cluster import performance when loading files into Spark?

- [ ] A) One giant 100 GB uncompressed CSV imports faster on a multi-node cluster than 1,000 files of 100 MB, because the cluster avoids the per-file metadata lookups entirely.
- [ ] B) Many smaller, similarly-sized files beat one giant file because more workers share the import; wildcards let one `spark.read.csv()` cover them all.
- [ ] C) Wildcard characters such as `*`, `?` and `[5-8]` are not supported by `spark.read.csv()`, so each file needs its own read statement followed by a chain of unions.
- [ ] D) With one giant CSV and no OS splitting tools available, all the complex joins and UDFs should be applied before writing to Parquet so the costly import happens once.

---

**47.** Fill in the blanks to split a large file into 10,000-line chunks with numeric suffixes using the Linux `split` utility, and to load multiple wildcard files in PySpark:

```bash
# 1. Split 'largefile' into 10,000-line chunks with numeric suffixes prefixed with 'chunk-'
split ______ 10000 ______ largefile chunk-
```
```python
# 2. Read all matching airport-*.txt.gz files in one statement
airport_df = spark.read.csv("______", header=True)
```

---

**48.** Standard Python plotting libraries cannot plot a PySpark DataFrame directly. Which statements about the three plotting approaches are true?

* **I.** `pyspark_dist_explore` pushes the binning into Spark and returns only summary data to the driver.
* **II.** `toPandas()` pulls the entire DataFrame into driver memory, which is dangerous on large raw datasets.
* **III.** `HandySpark`'s `toHandy()` offers pandas-like ergonomics while keeping the computation distributed.
* **IV.** No plotting library reads a distributed DataFrame directly, so a bridge of some kind is always needed.

- [ ] A) I and IV only
- [ ] B) II and III only
- [ ] C) All four
- [ ] D) I, II and III

---

**49.** Fill in the blanks to plot histograms from a PySpark DataFrame using `pyspark_dist_explore`, `toPandas()`, and `HandySpark`:

```python
import matplotlib.pyplot as plt
from pyspark_dist_explore import ______
from handyspark import *

# Method 1: pyspark_dist_explore
avg_ovr_df = fifa_21_avg_ovr.select('avg_ovr')
fig, ax = plt.subplots()
______(ax, avg_ovr_df, bins=20, color=['red'])
plt.show()

# Method 2: Convert to a pandas DataFrame first
fifa_pd = fifa_21_avg_pot.______()
fifa_pd['avg_pot'].hist(bins=20)
plt.show()

# Method 3: Wrap as a HandySpark DataFrame
hdf = test_df.______()
hdf.cols['Age'].hist()
plt.show()
```

---

**50.** Which of these disadvantages of caching DataFrames are described in the notes?

* **I.** Very large datasets may not fit in the memory reserved for cached DataFrames.
* **II.** Calling `.cache()` prevents any further transformations being applied to that DataFrame.
* **III.** The lifetime of a cached object is not guaranteed, and regenerating it can cause delays.
* **IV.** If the cache spills to a slow spinning disk, it may be no performance improvement at all.

- [ ] A) I and II only
- [ ] B) II and IV only
- [ ] C) All four
- [ ] D) I, III and IV

---

**51.** Fill in the blanks to benchmark a cached DataFrame and to write an intermediate Parquet representation:

```python
import ______

start_time = time.time()

# 1. Keep only the unique rows of departures_df and mark the result for in-memory caching
departures_df = departures_df.______().______()

# 2. Trigger the cache with an action, timing the first (uncached) run
print("Counting %d rows took %f seconds" % (departures_df.______(), time.time() - start_time))

# 3. If caching alone doesn't help: write an intermediate representation to disk, then read it back and cache that
voter_df.write.______('voter_data_intermediate.parquet', mode='overwrite')
voter_df = spark.read.______('voter_data_intermediate.parquet').______()
```

---

**52.** A Spark cluster has one driver and as many workers as required. Which statements match the responsibilities and sizing advice in the notes?

* **I.** The driver assigns tasks, monitors process state, handles retries and consolidates results.
* **II.** The driver is usually given roughly double the memory of the other nodes in the cluster.
* **III.** Every worker must be sized identically to the driver, otherwise task retries will fail.
* **IV.** More smaller workers often beat fewer larger ones, especially for import and export work.

- [ ] A) I, II and IV
- [ ] B) I and III only
- [ ] C) II, III and IV
- [ ] D) All four

---

**53.** Fill in the blanks to read and write Spark configuration settings at run time:

```python
# 1. Read the name of the Spark application instance
app_name = spark.conf.______("spark.app.name")

# 2. Read the driver's TCP port
driver_tcp_port = spark.conf.______("spark.driver.port")

# 3. Read the number of shuffle / join partitions
num_partitions = spark.conf.get('______')

# 4. Change the number of shuffle partitions to 500
spark.conf.______("spark.sql.shuffle.partitions", 500)
```

---

**54.** What is the difference between `.repartition(n)` and `.coalesce(n)`, and what happens if you call `.coalesce()` with a number **larger** than the current partition count?

- [ ] A) `.repartition()` only reduces the partition count while `.coalesce()` only increases it, and coalescing upward triggers the full shuffle that repartitioning avoids.
- [ ] B) `.repartition(n)` forces a full shuffle and can raise or rebalance partitions; `.coalesce(n)` reduces them without one, and coalescing upward is a no-op.
- [ ] C) They are identical aliases for the same underlying operation and only the argument name differs, so either may be used interchangeably to raise or lower the count.
- [ ] D) `.coalesce()` is an action that writes each partition straight to disk, whereas `.repartition()` is a lazy transformation that merely records the requested count.

---

**55.** Fill in the blanks to inspect an execution plan, control partitioning, and broadcast the smaller side of a join:

```python
from pyspark.sql.functions import ______

# 1. View the estimated execution plan for a distinct query
voter_df.select('name').distinct().______()

# 2. Force a full shuffle into 8 partitions
departures_df = departures_df.______(8)

# 3. Reduce to 2 partitions without a full shuffle
departures_df = departures_df.______(2)

# 4. Join flights_df with the much smaller airports_df, sending a copy of the small DataFrame to every worker
broadcast_df = flights_df.join(
    ______(airports_df),
    flights_df["Destination Airport"] == airports_df["IATA"]
)
```

---

**56.** Fill in the blanks to complete a small end-to-end data pipeline (read → filter → add ID → write JSON):

```python
import pyspark.sql.functions as F

# 1. Import 2015-departures.csv.gz, treating the first row as column names
departures_df = spark.read.csv("2015-departures.csv.gz", ______=True)

# 2. Keep only flights with a duration over 0 minutes
departures_df = departures_df.______(departures_df["Actual elapsed time (Minutes)"] > 0)

# 3. Add a unique ID column
departures_df = departures_df.withColumn('id', F.______())

# 4. Write the result out as a JSON document named output.json, replacing any existing output
departures_df.write.______("output.json", ______="overwrite")
```

---

**57.** In the notes, `flights_df.join(airports_df, ...)` and `flights_df.join(broadcast(airports_df), ...)` produced the **same** `BroadcastHashJoin` plan. Which statements are true?

* **I.** `broadcast()` gives each worker its own copy, cutting inter-node communication during a join.
* **II.** `broadcast()` must be applied to both sides of a join, or Spark raises an `AnalysisException`.
* **III.** Spark often applies this optimisation automatically, which is why the two plans matched here.
* **IV.** Broadcasting can slow things down on very small DataFrames, or if the larger side is broadcast.

- [ ] A) I and II only
- [ ] B) III and IV only
- [ ] C) I, III and IV
- [ ] D) All four

---

**58.** Fill in the blanks to import the `annotations.csv.gz` file, count its commented rows, and re-import it without them:

```python
from pyspark.sql.functions import col

# 1. Import annotations.csv.gz with a separator of | so each whole row lands in a single column
annotations_df = spark.read.csv('annotations.csv.gz', ______="|")
full_count = annotations_df.count()

# 2. Count the rows whose single column begins with a # character
comment_count = annotations_df.filter(col('______').______('#')).count()

# 3. Re-import the file, this time telling the CSV parser to drop commented rows
no_comments_df = spark.read.csv("annotations.csv.gz", sep="|", ______="#")
```

---

**59.** Which statements about Spark's CSV parser options for unconventional data are true?

* **I.** Blank lines are removed automatically by the CSV parser unless it is told otherwise.
* **II.** The `comment` option drops lines that *begin* with the given character.
* **III.** Choosing a `sep` that does not appear in the data causes the parser to raise an error.
* **IV.** When a schema is defined, the header row is not used as data and its names are ignored.

- [ ] A) I and III only
- [ ] B) II, III and IV
- [ ] C) All four
- [ ] D) I, II and IV

---

**60.** Fill in the blanks to count fields per row, drop malformed rows, split out the named columns, and return the variable-length remainder with a UDF:

```python
import pyspark.sql.functions as F
from pyspark.sql.types import ArrayType, StringType

# 1. Split _c0 on the tab character, then store the number of resulting fields in a 'colcount' column
tmp_fields = F.______(annotations_df['_c0'], "\t")
annotations_df = annotations_df.withColumn('colcount', F.______(tmp_fields))

# 2. Filter out any rows containing fewer than 5 fields
annotations_df_filtered = annotations_df.filter(______ (annotations_df['colcount'] < 5))

# 3. Pull the first two fields out into named columns
split_cols = F.split(annotations_df['_c0'], '\t')
split_df = annotations_df.withColumn('folder', split_cols.______(0))
split_df = split_df.withColumn('filename', split_cols.______(1))
split_df = split_df.withColumn('split_cols', split_cols)

# 4. Return every field from index 4 up to colcount as a list of strings
def retriever(cols, colcount):
    return cols[4:colcount]

udfRetriever = F.______(retriever, ______(______()))
split_df = split_df.withColumn('dog_list', udfRetriever(split_df.split_cols, split_df.colcount))
```

---

**61.** Which of these are given in the notes as typical data-validation checks?

* **I.** Casting every column to `StringType()` so that no type errors can occur downstream.
* **II.** Row counts landing within a tolerance, such as 2% of the previous month's row count.
* **III.** Data types matching the expectation for each column.
* **IV.** Content rules where no schema is enforced, such as a field being 9 characters or fewer.

- [ ] A) II, III and IV
- [ ] B) I and II only
- [ ] C) I, III and IV
- [ ] D) All four

---

**62.** Fill in the blanks with the join type that implements each validation goal against a known-good reference set:

```python
parsed_df = spark.read.parquet("/data/parsed_data.parquet")
company_df = spark.read.parquet("/data/known_companies.parquet")

# 1. Keep only the rows of parsed_df whose company also appears in the reference set
verified_df = parsed_df.join(company_df, on="company", how="______")

# 2. Find the rows of parsed_df that are MISSING from the reference set
invalid_df = parsed_df.join(company_df, on="company", how="______")

# 3. Keep every row of parsed_df, flagging which ones matched
flagged_df = parsed_df.join(company_df, on="company", how="______")
```

---

**63.** The notes recommend implementing complex validation rules as a **UDF**. Why?

- [ ] A) UDFs execute faster than every built-in Spark function because Catalyst inlines them into the physical plan, so all validation logic belongs in a UDF by default.
- [ ] B) A UDF keeps the rule in one place and can reach external sources — a web service, local files, API calls — that built-in functions cannot.
- [ ] C) Only a UDF may be referenced inside a `.filter()` clause, so any validation rule that has to remove rows must be expressed as a registered user-defined function.
- [ ] D) UDFs bypass lazy evaluation entirely, so a validation failure surfaces the moment the function is defined rather than when the pipeline's first action is reached.

---

**64.** Fill in the blanks to complete the pipeline pattern "define a schema → read a datafile → add an ID → write out two data types":

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import monotonically_increasing_id

# 1. Define a two-column schema in which neither field is allowed to be null
schema = StructType([
    StructField("name", StringType(), ______),
    StructField("age", IntegerType(), False)
])

# 2. Read the input file using that schema
df = spark.read.______("csv").______(schema).______("datafile.csv")

# 3. Add an ID column
df = df.withColumn("id", ______())

# 4. Write the same DataFrame out in two formats
df.write.format("______").save("output.csv")
df.write.format("______").save("output.parquet")
```

---

**65.** In Spark, what exactly *is* a data pipeline?

- [ ] A) It is a formally defined `Pipeline` object living in `pyspark.sql` which has to be instantiated and have stages added to it before any transformation will execute.
- [ ] B) It is another name for the physical plan that `.explain()` prints, which is why inspecting the plan is described as inspecting the pipeline's stages in order.
- [ ] C) A pipeline is a concept, not a formal object — just the code taking an input source to the desired output; distinct from Spark.ML's `Pipeline`.
- [ ] D) It is a cluster-level setting controlled through `spark.conf.set("spark.pipeline.stages", n)`, capping how many transformations may queue before an action runs.

---

**66.** Fill in the blanks to load `AA_DFW_2018.csv.gz` using the format/options/load style and add a lowercase airport column:

```python
import pyspark.sql.functions as F

# 1. Load the file, treating the first row as a header
aa_dfw_df = spark.read.______('csv') \
    .______(header=True) \
    .______('AA_DFW_2018.csv.gz')

# 2. Add an 'airport' column holding the lowercase version of 'Destination Airport'
aa_dfw_df = aa_dfw_df.withColumn(
    'airport',
    F.______(aa_dfw_df['Destination Airport'])
)
```

---

**67.** Which statements about Spark cluster types and where configuration lives are true?

* **I.** A cluster may be single node, standalone, or managed by YARN, Mesos or Kubernetes.
* **II.** Configuration can only be changed by editing config files and restarting the cluster.
* **III.** `spark.conf.set()` changes a setting from run-time code.
* **IV.** "Standalone" means one machine, while "single node" means several machines sharing a driver.

- [ ] A) II and IV only
- [ ] B) I, II and III
- [ ] C) I, III and IV
- [ ] D) I and III only

---

**68.** Fill in the blanks to rename a column using the dictionary form, overwrite a Parquet file, and confirm the result:

```python
# 1. Rename the column "_c3" to "flight_duration", passing a dictionary of old -> new names
df3_renamed = df3.______(
    {
        "_c3": "flight_duration"
    }
)

# 2. Save to 'AA_DFW_ALL.parquet', replacing any existing output
df3_renamed.write.______('AA_DFW_ALL.parquet', ______='overwrite')

# 3. Read it back and confirm the new column name and its type
flights_df = spark.read.parquet("AA_DFW_ALL.parquet")
flights_df.______()
```

---

**69.** Which statements about shuffling in Spark are true?

* **I.** It is the moving of data fragments between workers as required to complete certain tasks.
* **II.** It hides overall complexity from the user, who need not know which nodes hold which data.
* **III.** It occurs only when `.collect()` is called, so avoiding `collect()` eliminates it entirely.
* **IV.** It lowers the cluster's overall throughput, because workers sit waiting for transfers.

- [ ] A) I, II and IV
- [ ] B) I and IV only
- [ ] C) II and III only
- [ ] D) All four

---

**70.** Fill in the blanks to inspect distinct voter names in full and count the unique entries:

```python
# 1. (DallasCouncilVoters.csv.gz) Show 40 distinct VOTER_NAME values without cutting the strings short
voter_df.select('VOTER_NAME').______().show(40, ______=False)

# 2. (DallasCouncilVotes.csv.gz) Select the unique entries of the "VOTER NAME" column
unique_voters_df = df.select(df["VOTER NAME"]).______()

# 3. Count the rows in the result
print("\nThere are %d rows in the voter_df DataFrame.\n" % unique_voters_df.______())
```

---

**71.** The same 36-row DataFrame was given `ROW_ID` values via `F.monotonically_increasing_id()` twice. As a **1-partition** DataFrame the highest IDs were `35, 34, 33…`; as a **2-partition** DataFrame they were `8589934609, 8589934608, 8589934607…`. Why?

- [ ] A) The two-partition version silently retained duplicate rows that the single-partition version had removed, and those extra rows pushed the maximum ID far higher.
- [ ] B) Each partition owns a disjoint range of the 64-bit ID space, so partition 1 starts at 8589934592 (2³³); with one partition the IDs run from 0.
- [ ] C) `monotonically_increasing_id()` multiplies each row's position by the current partition count, so doubling the partitions scales every ID by roughly that factor.
- [ ] D) The IDs are random 64-bit integers drawn independently on each run, so two executions of the same code are unrelated and the observed values carry no structure.

---

**72.** Fill in the blanks for the "read once, write out as Parquet" technique, used when a single large file cannot be split with OS utilities:

```python
# 1. Do a slow one-off import of the single large file, supplying a defined schema
df_csv = spark.read.csv("singlelargefile.csv", header=True, ______=departures_schema)

# 2. Write it straight back out as Parquet, doing as little processing as possible first
df_csv.write.______("data.parquet", ______="overwrite")

# 3. Use the Parquet version for all later work
df = spark.read.______("data.parquet")
```

---

**73.** How do `.sort()` and `.orderBy()` relate, and how is descending order requested?

- [ ] A) `.sort()` sorts ascending only and `.orderBy()` descending only, so a multi-column sort mixing both directions has to be built from two chained calls.
- [ ] B) `.orderBy()` is an action that returns an ordered Python list to the driver, whereas `.sort()` is the lazy transformation that leaves the result distributed.
- [ ] C) Both mirror SQL's `ORDER BY` and default to ascending; descending comes from `desc()` or the column's own `.desc()` method.
- [ ] D) Descending order is requested with `.sort(reverse=True)`, matching the keyword argument Python's own built-in `sorted()` accepts for exactly the same purpose.

---

**74.** Fill in the blanks to compute an analysis value with a UDF, then with the preferred inline column arithmetic:

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import ______

# 1. A UDF that averages the sale values held in a list column
def getAvgSale(saleslist):
    total = 0
    count = len(saleslist)
    for sale in saleslist:
        total += sale[2] + sale[3]
    return total / count

udfGetAvgSale = ______(getAvgSale, ______())
df = df.withColumn("avg_sale", udfGetAvgSale(df.sales_list))

# 2. The same kind of calculation done inline - preferred wherever possible
df = df.withColumn("avg", (df.total_sales ______ df.sale_count))
df = df.withColumn("sq_ft", df.width ______ df.length)

# 3. A UDF result mixed straight into an inline calculation
df = df.withColumn("price_sqft", udfGetAvgSale(df.sales_list) ______ df.sq_ft)
```

---

**75.** Which statements correctly describe RDDs and DataFrames?

* **I.** RDDs are mutable, which is what makes them faster than DataFrames.
* **II.** RDDs lack schema-aware optimisations and need more lines of code for the same operation.
* **III.** DataFrames are schema-aware and benefit from the Catalyst optimiser.
* **IV.** DataFrames cannot be converted back into RDDs once they have been created.

- [ ] A) I and IV only
- [ ] B) II, III and IV
- [ ] C) I, II and III
- [ ] D) II and III only

---

**76.** Fill in the blanks to convert a DataFrame to an RDD and inspect it safely:

```python
df_salaries = spark.read.csv("salaries.csv", header=True, inferSchema=True)

# 1. Convert the DataFrame to an RDD (note: attribute, not a method!)
rdd_salaries = df_salaries.______

# 2. Retrieve ALL elements back to the driver as a list of Row objects
all_rows = rdd_salaries.______()

# 3. Safer alternative on a large RDD — grab just the first 5 elements
sample = rdd_salaries.______(5)

# 4. Check how many partitions the underlying RDD has
print(df_salaries.rdd.______())
```

---

**77.** For aggregating a dataset by key, how do the RDD and DataFrame approaches compare?

- [ ] A) The RDD route needs `map()` to build tuples then `reduceByKey()`, whereas the DataFrame API does it in one line and gets Catalyst optimisations.
- [ ] B) RDDs expose a one-line `groupBy().sum()` equivalent of their own, which makes them the more concise of the two options for straightforward key-based aggregation.
- [ ] C) `reduceByKey()` is available only on DataFrames and not on RDDs, so an aggregation written against an RDD has to be converted with `.toDF()` beforehand.
- [ ] D) RDD aggregations run faster in every case because they skip the Catalyst optimiser, trading schema awareness for a shorter path to the physical execution plan.

---

**78.** Fill in the blanks to aggregate the same data via an RDD and then via the DataFrame API:

```python
data = [("HR", "3000"), ("IT", "4000"), ("Finance", "3500")]
columns = ["Department", "Salary"]
df = spark.createDataFrame(data, schema=columns)

# 1. Map the DataFrame to an RDD of (Department, Salary) tuples
rdd = df.______.______(lambda row: (row["Department"], row["Salary"]))

# 2. Combine the values belonging to each key
rdd_aggregated = rdd.______(lambda x, y: x + y)

# 3. Bring the results back to the driver
print(rdd_aggregated.______())

# 4. The equivalent DataFrame one-liner
df.______("Department").______("Salary").show()
```

---

**79.** `.persist()` accepts a storage level. Which pairings of level and behaviour are correct?

* **I.** `MEMORY_ONLY` — the default used by `.cache()`; fastest, but needs enough RAM.
* **II.** `MEMORY_ONLY_SER` — stores objects uncompressed, using less CPU than the unserialised form.
* **III.** `MEMORY_AND_DISK` — spills to disk when memory runs out.
* **IV.** `DISK_ONLY` — stores entirely on disk; slower, but memory-safe.

- [ ] A) I and II only
- [ ] B) I, III and IV
- [ ] C) II and IV only
- [ ] D) All four

---

**80.** Fill in the blanks to aggregate with the dictionary form of `agg()` and across *all* rows:

```python
# 1. Group by company size and compute the average salary, passing a dictionary to agg()
salaries_df.______("company_size").______({"salary_in_usd": "avg"}).show()

# 2. Find the minimum salary across ALL rows at small companies — note the empty grouping
salaries_df.filter(salaries_df.company_size == "S").______().______("salary_in_usd").show()

# 3. Find the maximum salary across all rows at large companies
salaries_df.filter(salaries_df.company_size == "L").groupBy().______("salary_in_usd").show()
```

---

**81.** Which of these are listed as best practices for PySpark aggregations?

* **I.** Filter early, reducing the data size before aggregating.
* **II.** Handle data types — cast strings to numerics before aggregating.
* **III.** Minimise heavy whole-dataset operations such as `.groupBy()` where possible.
* **IV.** Use `.explain()` to inspect the execution plan and optimise accordingly.

- [ ] A) I and III only
- [ ] B) II and IV only
- [ ] C) All four
- [ ] D) I, II and III

---

**82.** Fill in the blanks to build DataFrames from `Row` objects and from a pandas DataFrame:

```python
from pyspark.sql import ______
from datetime import datetime, date

# 1. Build a DataFrame from objects with named fields
df = spark.createDataFrame([
    ______(a=1, b=2., c='string1', d=date(2000, 1, 1), e=datetime(2000, 1, 1, 12, 0)),
    ______(a=2, b=3., c='string2', d=date(2000, 2, 1), e=datetime(2000, 1, 2, 12, 0))
])

# 2. Print the column names, types and nullability as a tree
df.______()

# 3. Build a DataFrame from an existing pandas DataFrame instead
age_category_df = spark.______(pd_df)
```

---

**83.** Which statements about registering a plain Python function with `udf(fn, ReturnType())` are true?

* **I.** Registration to the `SparkSession` is what makes the function available to every worker node.
* **II.** The second argument declares the Spark return type, and should always be specified explicitly.
* **III.** Registration compiles the Python function to Scala bytecode so that it executes on the JVM.
* **IV.** The second argument sets how many rows are batched together and sent across to Python.

- [ ] A) III and IV only
- [ ] B) I, II and III
- [ ] C) I and IV only
- [ ] D) I and II only

---

**84.** Fill in the blanks to register a temp view and run a grouped, rounded, sorted SQL aggregation on it:

```python
# 1. Register the fifa21 DataFrame as a temporary view named 'fifa_view'
fifa21.______('fifa_view')

# 2. Average the Overall rating per club, rounded to 0 decimal places, highest first
fifa_21_avg_ovr = spark.______(
    """
    SELECT Club, ______(AVG(Overall), 0) as avg_ovr
    FROM fifa_view
    ______ Club
    ______ avg_ovr DESC
    """
)
```

---

**85.** Which statements comparing a pandas DataFrame with a PySpark DataFrame are correct?

* **I.** pandas runs on a single machine, bounded by that one server's memory.
* **II.** pandas has the narrower API of the two, which is why PySpark covers more operations.
* **III.** pandas DataFrames are mutable in place, whereas PySpark DataFrames are immutable.
* **IV.** pandas evaluates eagerly, whereas PySpark queues transformations until an action runs.

- [ ] A) I, III and IV
- [ ] B) I and II only
- [ ] C) II and IV only
- [ ] D) All four

---

**86.** Fill in the blanks to summarise and filter a temporary view with SQL, using several aggregates in one query:

```python
# 1. Register the DataFrame as a temporary table
test_df.______("test_table")

# 2. Total and maximum purchase per age group
query = """
    SELECT Age,
           ______(Purchase) AS total_purchase,
           ______(Purchase) AS max_purchase
    FROM test_table
    ______ Age
"""
test_agg_df = spark.sql(query)

# 3. Female high spenders - two conditions combined
query = """
    SELECT Age, Purchase, Gender
    FROM test_table
    ______ Gender = 'Female'
      ______ Purchase > 20000
"""
```

---

**87.** Reading `people.csv` with `header=True, inferSchema=True` produced a first column named `_c0` of type integer, plus the warning `CSV header does not conform to the schema. Header: , person_id, name, sex, date of birth ... Expected: _c0 but found:`. What happened?

- [ ] A) `inferSchema=True` always renames the leading column to `_c0` regardless of the header row, because type inference runs before any header names are applied.
- [ ] B) The file's first header cell is empty — an unnamed index column — so Spark fell back to its default positional name `_c0`.
- [ ] C) The file carries more data columns than header names, so Spark appended `_c0` at the end of the schema to account for the extra unnamed trailing column.
- [ ] D) `header=True` was ignored because the file spans more than one partition, and Spark honours the header option only when the file fits in a single partition.

---

**88.** Fill in the blanks to measure a join with and without broadcasting, then confirm which strategy Spark chose:

```python
import time
from pyspark.sql.functions import ______

# 1. Join without broadcasting, timing the count
normal_df = flights_df.join(airports_df, flights_df["Destination Airport"] == airports_df["IATA"])
start_time = time.time()
normal_count = normal_df.______()
normal_duration = time.time() - start_time

# 2. Join again with the smaller DataFrame broadcast, timed the same way
broadcast_df = flights_df.join(______(airports_df), flights_df["Destination Airport"] == airports_df["IATA"])
start_time = time.time()
broadcast_count = broadcast_df.count()
broadcast_duration = time.time() - start_time

# 3. Confirm which join strategy Spark actually picked
broadcast_df.______()
```

---

**89.** A cloud job finishes in 1 hour on 16 worker nodes for about $50; on 8 workers it takes 1.25 hours and costs roughly half as much. What do the notes conclude?

- [ ] A) Worker count should always be maximised, because the per-node hourly rate falls as more nodes join the cluster and so the total bill falls with it.
- [ ] B) The 16-node run is strictly better, since halving the workers more than doubled the runtime and Spark's scaling is linear in the number of workers.
- [ ] C) The fastest configuration is not necessarily the best value, so sizing should be tested against both runtime and cost for the actual workload.
- [ ] D) Doubling the workers should have halved the runtime exactly, so the 1.25-hour result points to a misconfigured driver that needs twice the memory.

---

**90.** Fill in the blanks to read non-CSV formats — JSON, plus Parquet both the long way and via the shortcut:

```python
# 1. Load the semi-structured adults.json file
census_df = spark.read.______("adults.json")

# 2. Keep only the rows where age is over 40
salary_filtered_census = census_df.______(census_df['age'] > 40)

# 3. Read a Parquet file the long way, using the generic reader
df = spark.read.______('parquet').______('AA_DFW_ALL.parquet')

# 4. Read the same Parquet file using the shortcut
df = spark.read.______('AA_DFW_ALL.parquet')
```

---

**91.** `canada_titles.describe().show()` on a single **string** column `job_title` returned count `1115`, mean `NULL`, stddev `NULL`, min `AI Architect`, max `Statistical Progr...`. Why?

- [ ] A) `describe()` failed outright because string columns are unsupported, so the min and max shown are placeholders taken from the first and last rows that were read.
- [ ] B) The nulls report that `job_title` holds 1115 missing values, which is also why the count matches the row total rather than the number of distinct job titles.
- [ ] C) `describe()` works only once `inferSchema=True` has cast the column to a numeric type, so the nulls indicate that cast silently failed on the non-numeric strings.
- [ ] D) `describe()` reports count, mean, stddev, min and max; mean and stddev are undefined for strings, and min/max fall back to alphabetical order.

---

**92.** Fill in the blanks to query a temporary view with SQL and summarise the result:

```python
# 1. Create a temporary table "salaries_table" from the salaries_df DataFrame
salaries_df.______("salaries_table")

# 2. Extract the "job_title" column for companies located in Canada
query = """
SELECT job_title
FROM salaries_table
______ company_location == "CA"
"""

# 3. Run the query — the result is itself a DataFrame
canada_titles = spark.______(query)

# 4. Get a statistical summary of the table
canada_titles.______().show()
```

---

**93.** What does `salaries_df.filter(...).groupBy().avg("salary_in_usd")` do, with `groupBy()` called with **no arguments**?

- [ ] A) An empty `groupBy()` treats the whole filtered DataFrame as one group, returning a single-row result such as `avg(salary_in_usd)`.
- [ ] B) It raises a `TypeError`, because `groupBy()` requires at least one column name in order to build the grouping expression the aggregate is then applied over.
- [ ] C) It groups by every column in the DataFrame at once, which makes the call equivalent to `dropDuplicates()` followed by the requested aggregate function.
- [ ] D) It silently returns the original filtered DataFrame unchanged, since with nothing to group by Spark has no grouping key to attach the aggregate result to.

---

**94.** Fill in the blanks to register a session-scoped view, a cross-session view, and query the latter:

```python
salaries = spark.read.csv("salaries.csv", header=True, inferSchema=True)

# 1. Register a view that lives only for the current Spark session
salaries.______("salaries")

# 2. Register a view that persists across sessions within the same Spark application
salaries.______("salaries_global")

# 3. Query the cross-session view via the database it lives in
spark.sql("SELECT * FROM ______.salaries_global LIMIT 5").show()
```

---

**95.** Which statements about the `@udf` decorator form are true?

* **I.** The decorator form runs inside the JVM, so it avoids the per-row serialisation cost.
* **II.** `@udf(StringType())` is syntactic sugar for `fn = udf(StringType())(fn)`.
* **III.** `@udf` with no parentheses defaults the return type to `StringType()`.
* **IV.** Spark SQL type strings such as `"int"` or `"array<string>"` work in place of typed classes.

- [ ] A) I and II only
- [ ] B) II, III and IV
- [ ] C) I, III and IV
- [ ] D) All four

---

**96.** Fill in the blanks to define and apply an **argument-less** UDF:

```python
import random
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

# 1. A UDF that takes no column input at all
def sortingCap():
    return random.______(['G', 'H', 'R', 'S'])

udfSortingCap = F.udf(sortingCap, ______())

# 2. Apply it — note the empty parentheses, since no column is passed in
user_df = user_df.withColumn('House', ______())
```

---

**97.** Running `F.split(voter_df.VOTER_NAME, '\s+')` and `voter_df.splits.getItem(F.size('splits') - 1)` produced a `SyntaxWarning: invalid escape sequence '\s'` and a `FutureWarning: A column as 'key' in getItem is deprecated as of Spark 3.0`. What are these telling you?

- [ ] A) Both are Spark errors that abort the transformation, which is why the `first_name` and `last_name` columns were left unpopulated in the resulting DataFrame.
- [ ] B) The `SyntaxWarning` means Spark could not split on whitespace at all, so the `splits` column holds each full voter name as a single unsplit array element.
- [ ] C) The first is a Python warning — `'\s'` is not a valid escape, so prefer `r'\s+'`; the second is a PySpark deprecation favouring `column[key]`.
- [ ] D) The `FutureWarning` means `F.size()` is being withdrawn from `pyspark.sql.functions`, and array lengths must instead come from the column's `.length` attribute.

---

**98.** Fill in the blanks to demonstrate immutability and lazy processing — each step rebinds the name to a *new* DataFrame, and nothing runs until the action:

```python
# 1. Create a DataFrame from a CSV
voter_df = spark.read.csv('voterdata.csv')

# 2. Add a 'fullyear' column — this does not mutate voter_df; it creates a NEW DataFrame
voter_df = voter_df.______(
    'fullyear',
    voter_df['year'] + 2000
)

# 3. Remove the original 'year' column — again a new DataFrame; the old definition is discarded
voter_df = voter_df.______('year')

# 4. Nothing above has actually run yet. Trigger the whole chain with an action
voter_df.______()
```

---

**99.** A cached DataFrame's `.explain()` output contained `Exchange hashpartitioning(Department#1378, 200)`, two `HashAggregate(...)` stages, and an `InMemoryTableScan` over an `InMemoryRelation ... StorageLevel(disk, memory, deserialized, 1 replicas)`. What do these tell you?

- [ ] A) Nothing useful — `.explain()` only echoes the Python source of the query back to the console, which is why reading it requires the original script alongside.
- [ ] B) `InMemoryTableScan` indicates the DataFrame failed to cache, so Spark silently fell back to re-scanning the original source file from disk for each aggregate stage.
- [ ] C) The `200` is Spark's own estimate of how many rows the aggregation will return, which is how the optimiser then decides whether a broadcast join would be worthwhile.
- [ ] D) `Exchange hashpartitioning` is the shuffle across 200 partitions, the two `HashAggregate` stages are partial and final, and `InMemoryTableScan` means cache was used.

---

**100.** Fill in the blanks to query a Parquet file with SQL and pull the single aggregate value back to the driver:

```python
# 1. Read the Parquet file into a DataFrame
flights_df = spark.read.______("AA_DFW_ALL.parquet")

# 2. Register it as a temporary table named 'flights'
flights_df.______('flights')

# 3. Query the average flight duration, then retrieve the single result row
avg_duration = spark.sql('SELECT ______(flight_duration) from flights').______()[0]
print('The average flight time is: %d' % avg_duration)
```
