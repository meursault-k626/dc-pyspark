**1.**
- [x] **D) Veracity**
*(The 3 V's covered in the notes are **Volume**, **Variety**, and **Velocity**.)*

---

**2.**
1. `Clustered`
2. `Parallel`
3. `Distributed`
4. `Batch`

---

**3.**
- [x] **B) Up to 100× faster in memory, and up to 10× faster on disk**

---

**4.**
1. `Scala`
2. `Spark Core`
3. `MLlib`, `GraphX`, `Streaming`

---

**5.**
- [x] **B) No code change is necessary; only the `master` URL configuration changes.**

---

**6.**
1. `pyspark`, `sparkR`
2. `version`, `pythonVer`, `master`

---

**7.**
- [x] **A) A `lambda` body is one expression returned implicitly; `map()` transforms every item (same output length) while `filter()` keeps only truthy items.**
*(A lambda is just a function written inline and nameless — needing `if`, loops or multiple lines means reaching for `def` instead.)*

---

**8.**
1. `map`
2. `filter`

---

**9.**
- [x] **C) Resilient — recomputes missing or damaged partitions; Distributed — spans jobs across cluster nodes; Datasets — a collection of partitioned data.**

---

**10.**
1. `parallelize`, `minPartitions`
2. `textFile`, `minPartitions`
3. `getNumPartitions`

---

**11.**
- [x] **A) `map` returns 2 list elements, `[['hello','world'], ['how','are','you']]`; `flatMap` flattens them into 5 string elements.**
*(`flatMap` is one-to-many: 2 input elements produced 5 output elements.)*

---

**12.**
1. `filter`, `filter`
2. `union`
3. `first`
4. `take`
5. `count`

---

**13.**
- [x] **C) `reduceByKey()` combines values locally on each partition before shuffling, whereas `groupByKey()` shuffles every key-value pair across the network.**
*(Combining locally first means far less data crosses the network — which is why `groupByKey()` is the expensive option at scale.)*

---

**14.**
1. `flatMap`
2. `filter`
3. `map`
4. `reduceByKey`
5. `sortByKey`, `False`

---

**15.**
- [x] **B) `[('L. Messi', (30, '€565K')), ('Neymar', (25, '€280K'))]`**

---

**16.**
1. `filter` (or `where`
2. `createOrReplaceTempView`
3. `sql`, `COUNT`, `GROUP BY`
4. `orderBy` (or `sort`

---

**17.**
- [x] **A) `SparkContext` is the entry point for RDDs; `SparkSession` is the unified entry point for DataFrames, temp views and SQL.**

---

**18.**
1. `SparkSession`
2. `builder`, `appName`, `getOrCreate`
3. `createDataFrame`
4. `header`, `inferSchema`

---

**19.**
- [x] **B) Spark reads the dot as nested struct field access; wrap the name in backticks — ``df.select("`education.num`")``.**
*(Renaming on load with `.toDF(...)` or `withColumnRenamed()` avoids the problem for the rest of the pipeline.)*

---

**20.**
1. `printSchema`
2. `columns`
3. `describe`
4. `dropDuplicates`
5. `withColumnRenamed`

---

**21.**
- [x] **A) I and III only**
*(`count()` and `collect()` are actions — they trigger execution of the queued plan.)*

---

**22.**
1. `filter` (or `where`), `filter` (or `where`), `avg`
2. `groupBy` (or `groupby`), `orderBy` (or `orderby` / `sort`), `desc`

---

**23.**
- [x] **D) Parquet files are plain-text files that can be opened and edited directly in Notepad, just like CSVs.**
*(Parquet is a compressed binary columnar format and cannot be edited in a plain text editor.)*

---

**24.**
1. `StructType`, `StructField`
2. `StructType`
3. `StructField`, `StructField`, `StructField`, `StructField`, `StructField`
4. `schema`

---

**25.**
- [x] **C) Without a schema Spark reads the data twice — once to infer types, once to load; an explicit schema needs one read and filters bad rows during it.**

---

**26.**
1. `na`, `drop`
2. `how`
3. `isNotNull`
4. `na`, `fill`
5. `withColumn`, `drop`

---

**27.**
- [x] **D) `union()` is positional — identical column count, types and order are required; for name-based union use `unionByName()`.**

---

**28.**
1. `withColumnRenamed`
2. `join`, `on`, `how`

---

**29.**
- [x] **A) A Struct has a fixed, named, typed schema read via dot notation; a Map holds per-row dynamic keys. An odd argument count raises an `AnalysisException`.**
*(Struct when the shape of the nested data is known and consistent; map when the keys themselves are data that varies.)*

---

**30.**
1. `array`, `create_map`, `struct`, `lit`
2. `array`, `lit`
3. `create_map`
4. `struct`
5. `address.city`

---

**31.**
- [x] **B) Standard UDFs run row-by-row with JVM↔Python serialisation cost and are opaque to Catalyst; pandas UDFs process batches as vectorised Series.**
*(Vectorisation is the point — pandas UDFs avoid the per-row JVM↔Python round trip, which is where standard UDFs lose their time.)*

---

**32.**
1. `pandas_udf`
2. `udf`, `StringType`
3. `pandas_udf`
4. `add_ten_pandas`

---

**33.**
- [x] **C) `createOrReplaceTempView()` registers a session-scoped view that dies with the session; `createGlobalTempView()` is cross-session, queried via `global_temp.`**

---

**34.**
1. `cast`
2. `createOrReplaceTempView`
3. `sql`

---

**35.**
- [x] **D) `.explain()` prints the logical and physical plans so redundant shuffles become visible; `broadcast(small_df)` sends a small table to every worker.**

---

**36.**
1. `StorageLevel`
2. `cache`
3. `is_cached`
4. `unpersist`
5. `persist`, `StorageLevel`

---

**37.**
- [x] **A) Immutability lets Spark share data safely across concurrent components with no locks; each transformation builds a new recipe, and nothing runs until an action.**

---

**38.**
1. `union`
2. `parquet`, `mode`
3. `read`, `parquet`

---

**39.**
- [x] **B) It filters during the file-read stage itself, so irrelevant chunks never leave disk; CSV cannot use it because the whole text file must be parsed first.**

---

**40.**
- [x] **C) I and III only**
*(`.cast()` and `.getItem()` are called on the column object — the classic F-versus-method trap.)*

---

**41.**
1. `length`, `length`
2. `~`, `contains`
3. `split`
4. `getItem`, `getItem`, `size`

---

**42.**
- [x] **D) Rows with `Age < 18` get `null`; any number of `.when()` clauses can be chained (first match wins) but at most one `.otherwise()` at the end.**

---

**43.**
1. `when`
2. `when`
3. `otherwise`

---

**44.**
- [x] **A) Spark can optimise inline column maths, whereas a UDF carries a real performance penalty; UDFs stay the fallback for logic the built-ins cannot express.**
*(Spark doesn't care where a column's values came from — a UDF result can be mixed straight into inline arithmetic, as in `udfGetAvgSale(df.sales_list) / df.sq_ft`.)*

---

**45.**
1. `repartition`
2. `max`
3. `monotonically_increasing_id`

---

**46.**
- [x] **B) Many smaller, similarly-sized files beat one giant file because more workers share the import; wildcards let one `spark.read.csv()` cover them all.**

---

**47.**
1. `-l`, `-d`
2. `airport-*.txt.gz`

---

**48.**
- [x] **C) All four**
*(Aggregate in Spark first, then bring the small result to pandas — the safest of the three routes.)*

---

**49.**
1. `hist`
2. `hist`
3. `toPandas`
4. `toHandy`

---

**50.**
- [x] **D) I, III and IV**
*(`.cache()` never blocks further transformations — it is itself a lazy transformation.)*

---

**51.**
1. `time`
2. `distinct`, `cache`
3. `count`
4. `parquet`, `parquet`, `cache`

---

**52.**
- [x] **A) I, II and IV**

---

**53.**
1. `get`
2. `get`
3. `spark.sql.shuffle.partitions`
4. `set`

---

**54.**
- [x] **B) `.repartition(n)` forces a full shuffle and can raise or rebalance partitions; `.coalesce(n)` reduces them without one, and coalescing upward is a no-op.**

---

**55.**
1. `broadcast`
2. `explain`
3. `repartition`
4. `coalesce`
5. `broadcast`

---

**56.**
1. `header`
2. `filter` (or `where`)
3. `monotonically_increasing_id`
4. `json`, `mode`

---

**57.**
- [x] **C) I, III and IV**

---

**58.**
1. `sep`
2. `_c0`, `startswith`
3. `comment`

---

**59.**
- [x] **D) I, II and IV**
*(An absent separator doesn't fail — the whole row lands in a single `_c0` column, which is the trick for parsing nested data.)*

---

**60.**
1. `split`, `size`
2. `~`
3. `getItem`, `getItem`
4. `udf`, `ArrayType`, `StringType`

---

**61.**
- [x] **A) II, III and IV**

---

**62.**
1. `inner`
2. `left_anti`
3. `left`

---

**63.**
- [x] **B) A UDF keeps the rule in one place and can reach external sources — a web service, local files, API calls — that built-in functions cannot.**

---

**64.**
1. `False`
2. `format`, `schema`, `load`
3. `monotonically_increasing_id`
4. `csv`, `parquet`

---

**65.**
- [x] **C) A pipeline is a concept, not a formal object — just the code taking an input source to the desired output; distinct from Spark.ML's `Pipeline`.**

---

**66.**
1. `format`, `options`, `load`
2. `lower`

---

**67.**
- [x] **D) I and III only**
*(Configuration is available in config files, the Spark web interface and run-time code.)*

---

**68.**
1. `withColumnsRenamed`
2. `parquet`, `mode`
3. `printSchema`

---

**69.**
- [x] **A) I, II and IV**

---

**70.**
1. `distinct`, `truncate`
2. `distinct`
3. `count`

---

**71.**
- [x] **B) Each partition owns a disjoint range of the 64-bit ID space, so partition 1 starts at 8589934592 (2³³); with one partition the IDs run from 0.**
*(2³³ = 8,589,934,592 — the first ID in partition 1's range.)*

---

**72.**
1. `schema`
2. `parquet`, `mode`
3. `parquet`

---

**73.**
- [x] **C) Both mirror SQL's `ORDER BY` and default to ascending; descending comes from `desc()` or the column's own `.desc()` method.**

---

**74.**
1. `FloatType`
2. `udf`, `FloatType`
3. `/`, `*`
4. `/`

---

**75.**
- [x] **D) II and III only**
*(`df.rdd` converts a DataFrame to an RDD and `rdd.toDF()` converts back.)*

---

**76.**
1. `rdd`
2. `collect`
3. `take`
4. `getNumPartitions`

---

**77.**
- [x] **A) The RDD route needs `map()` to build tuples then `reduceByKey()`, whereas the DataFrame API does it in one line and gets Catalyst optimisations.**

---

**78.**
1. `rdd`, `map`
2. `reduceByKey`
3. `collect`
4. `groupBy` (or `groupby`), `sum`

---

**79.**
- [x] **B) I, III and IV**
*(The `_SER` variants are serialised — more compact but more CPU-heavy, not less.)*

---

**80.**
1. `groupBy` (or `groupby`), `agg`
2. `groupBy` (or `groupby`), `min`
3. `max`

---

**81.**
- [x] **C) All four**

---

**82.**
1. `Row`
2. `Row`, `Row`
3. `printSchema`
4. `createDataFrame`

---

**83.**
- [x] **D) I and II only**
*(Complex return types are fine too — `ArrayType(StringType())`, or SQL strings like `"array<string>"`.)*

---

**84.**
1. `createOrReplaceTempView`
2. `sql`, `ROUND`, `GROUP BY`, `ORDER BY`

---

**85.**
- [x] **A) I, III and IV**

---

**86.**
1. `createOrReplaceTempView`
2. `SUM`, `MAX`, `GROUP BY`
3. `WHERE`, `AND`

---

**87.**
- [x] **B) The file's first header cell is empty — an unnamed index column — so Spark fell back to its default positional name `_c0`.**

---

**88.**
1. `broadcast`
2. `count`
3. `broadcast`
4. `explain`

---

**89.**
- [x] **C) The fastest configuration is not necessarily the best value, so sizing should be tested against both runtime and cost for the actual workload.**
*(More, smaller workers often beat fewer, larger ones — especially on import/export — but the notes stress testing configurations rather than assuming.)*

---

**90.**
1. `json`
2. `filter` (or `where`)
3. `format`, `load`
4. `parquet`

---

**91.**
- [x] **D) `describe()` reports count, mean, stddev, min and max; mean and stddev are undefined for strings, and min/max fall back to alphabetical order.**

---

**92.**
1. `createOrReplaceTempView`
2. `WHERE`
3. `sql`
4. `describe`

---

**93.**
- [x] **A) An empty `groupBy()` treats the whole filtered DataFrame as one group, returning a single-row result such as `avg(salary_in_usd)`.**

---

**94.**
1. `createOrReplaceTempView`
2. `createGlobalTempView`
3. `global_temp`

---

**95.**
- [x] **B) II, III and IV**

---

**96.**
1. `choice`, `StringType`
2. `udfSortingCap`

---

**97.**
- [x] **C) The first is a Python warning — `'\s'` is not a valid escape, so prefer `r'\s+'`; the second is a PySpark deprecation favouring `column[key]`.**
*(Both are warnings, not errors — the code still produced the expected `first_name` / `last_name` columns.)*

---

**98.**
1. `withColumn`
2. `drop`
3. `count`

---

**99.**
- [x] **D) `Exchange hashpartitioning` is the shuffle across 200 partitions, the two `HashAggregate` stages are partial and final, and `InMemoryTableScan` means cache was used.**

---

**100.**
1. `parquet`
2. `createOrReplaceTempView`
3. `avg`, `collect`
