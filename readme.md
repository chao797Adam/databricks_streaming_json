
# JSON Ingestion & Flattening with Auto Loader

This project ingests a nested JSON file representing **one order per record**, and flattens it into a Silver-layer table using Auto Loader and PySpark.

---

## 1. Source Data: One Order per JSON Record

The source is a nested JSON file. Each record represents **one order**.

```json
{
  "order_id": "ORD1001",
  "timestamp": "2025-06-01T10:15:00Z",
  "customer": {
    "customer_id": 501,
    "name": "John Doe",
    "email": "john@example.com",
    "address": {
      "city": "Toronto",
      "postal_code": "M5H 2N2",
      "country": "Canada"
    }
  },
  "items": [
    { "item_id": "I100", "product_name": "Wireless Mouse", "quantity": 2, "price": 25.99 },
    { "item_id": "I101", "product_name": "USB-C Adapter", "quantity": 1, "price": 15.49 }
  ],
  "payment": {
    "method": "Credit Card",
    "transaction_id": "TXN7890"
  },
  "metadata": [
    { "key": "campaign", "value": "back_to_school" },
    { "key": "channel", "value": "email" }
  ]
}
```

**Field types:**

| Field | Type | Notes |
| :--- | :--- | :--- |
| `order_id` | scalar | Top-level string |
| `timestamp` | scalar | Top-level string |
| `customer` | struct | 1-to-1 nested object |
| `items` | array of struct | 1-to-many |
| `payment` | struct | 1-to-1 nested object |
| `metadata` | array of struct | 1-to-many |

---

## 2. Inspection Workflow (Step-by-Step)

For nested JSON, **do not write the whole transformation in one go**. Inspect each step.

### Step 1: Batch read to infer structure

```python
df = (spark.read
  .format("json")
  .option("multiLine", True)
  .load("/Volumes/.../jsonsource")
)
display(df)
```

### Step 2: Print the inferred schema

```python
df.printSchema()
```

**Output (excerpt):**

```text
root
 |-- customer: struct (nullable = true)
 |    |-- address: struct (nullable = true)
 |    |    |-- city: string (nullable = true)
 |    |    |-- country: string (nullable = true)
 |    |    |-- postal_code: string (nullable = true)
 |    |-- customer_id: long (nullable = true)
 |    |-- email: string (nullable = true)
 |    |-- name: string (nullable = true)
 |-- items: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- item_id: string (nullable = true)
 |    |    |-- price: double (nullable = true)
 |    |    |-- product_name: string (nullable = true)
 |    |    |-- quantity: long (nullable = true)
 |-- metadata: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- key: string (nullable = true)
 |    |    |-- value: string (nullable = true)
 |-- order_id: string (nullable = true)
 |-- payment: struct (nullable = true)
 |    |-- method: string (nullable = true)
 |    |-- transaction_id: string (nullable = true)
 |-- timestamp: string (nullable = true)
```

**Reading it:**
- Scalars: `order_id`, `timestamp`
- Structs (1-to-1): `customer`, `payment`
- Arrays (1-to-many): `items`, `metadata`

---

## 3. Generating the DDL for Streaming

Streaming readers use an explicit schema to avoid repeated inference.

### Method A: Reuse the inferred schema object

```python
my_schema = df.schema
df_stream = spark.readStream.format("json") \
    .option("multiLine", True) \
    .schema(my_schema) \
    .load("/Volumes/.../jsonsource")
```

### Method B: Write a DDL string (for readability)

There is **no built-in API** to auto-generate a multi-line DDL from a `StructType`. It must be hand-written or AI-generated from `printSchema()`.

**Conversion rules:**

| Python type | DDL keyword |
| :--- | :--- |
| `StringType()` | `STRING` |
| `LongType()` | `BIGINT` |
| `DoubleType()` | `DOUBLE` |
| `StructType([...])` | `STRUCT<...>` |
| `ArrayType(StructType([...]))` | `ARRAY<STRUCT<...>>` |

**Syntax warning:** In DDL, field name and type are separated by **space**, not colon.
- ✅ `customer_id BIGINT`
- ❌ `customer_id: BIGINT`

**Generated DDL:**

```python
my_schema = """
    order_id STRING,
    timestamp STRING,
    customer STRUCT<
        customer_id BIGINT,
        name STRING,
        email STRING,
        address STRUCT<
            city STRING,
            postal_code STRING,
            country STRING
        >
    >,
    items ARRAY<STRUCT<
        item_id STRING,
        product_name STRING,
        quantity BIGINT,
        price DOUBLE
    >>,
    payment STRUCT<
        method STRING,
        transaction_id STRING
    >,
    metadata ARRAY<STRUCT<
        key STRING,
        value STRING
    >>
"""
```

### JSON Structure Primer: Object vs. Array

JSON uses two container types, and they require different handling:

| Symbol | Type | Example | Spark Handling |
| :--- | :--- | :--- | :--- |
| `{ }` | **Object** (dict) | `"customer": {"name": "John"}` | Access with `.` → `col("customer.name")` |
| `[ ]` | **Array** (list) | `"items": [{"item_id": "I100"}, ...]` | Explode with `explode()` |

**In this project:**
- `customer`, `customer.address`, `payment` → **objects** (structs) → access with `.`
- `items`, `metadata` → **arrays** → explode into separate rows

Recognizing the bracket type (`{` vs `[`) determines whether to use `.` or `explode()`.

### Quick Rule: `{ }` vs `[ ]`

When reading a JSON file, use the bracket type to decide how to handle the field:

| Bracket | Meaning | Spark Handling |
| :--- | :--- | :--- |
| `{ }` | Object (struct) | Access nested fields with `.` → `col("a.b")` |
| `[ ]` | Array (list) | Expand with `explode()` or `explode_outer()` |
| No bracket | Scalar | Access directly with `col("a")` |

**Rule of thumb:**
- **Can you use `explode` on it?** → It's an array (`[...]`).
- **Can you use `.` to drill into it?** → It's an object (`{...}`).

**Applied to this project:**

| Field | Bracket | Action |
| :--- | :--- | :--- |
| `customer` | `{...}` | `customer.name`, `customer.email`, ... |
| `customer.address` | `{...}` | `customer.address.city`, ... |
| `items` | `[...]` | `explode("items")` |
| `payment` | `{...}` | `payment.method`, ... |
| `metadata` | `[...]` | `explode("metadata")` |

**Note:** In JSON, `explode` is practically always applied to arrays. Maps exist in Spark but rarely appear in raw JSON, so the rule "explode = array" is accurate for 90%+ of real-world cases.

---

## 4. Transformation and Output

### 4.1 Streaming Read (Explicit Schema Required)

Unlike batch reads, `spark.readStream` **does not support automatic schema inference** in current Databricks runtimes. The config `spark.sql.streaming.schemaInference` is not available, so the schema must be provided explicitly.

```python
my_schema = """
    order_id STRING,
    timestamp STRING,
    customer STRUCT<
        customer_id BIGINT,
        name STRING,
        email STRING,
        address STRUCT<
            city STRING,
            postal_code STRING,
            country STRING
        >
    >,
    items ARRAY<STRUCT<
        item_id STRING,
        product_name STRING,
        quantity BIGINT,
        price DOUBLE
    >>,
    payment STRUCT<
        method STRING,
        transaction_id STRING
    >,
    metadata ARRAY<STRUCT<
        key STRING,
        value STRING
    >>
"""

df = (spark.readStream
  .format("json")
  .option("multiLine", True)
  .schema(my_schema)
  .load("/Volumes/workspace/stream/streaming/jsonsource")
)
```

> **Note:** `spark.sql.streaming.schemaInference = true` only works in some environments. In current Databricks runtimes, this config is **not available**, so an explicit schema is mandatory.

### 4.2 Transformation Workflow

The transformation follows the reference tutorial: **explode both `items` and `metadata`** to fully flatten the JSON.

#### Step 1: Select fields to work with

```python
df = df.select(
    "items", "order_id", "timestamp",
    "customer.customer_id", "customer.name", "customer.email",
    "customer.address.city", "customer.address.country", "customer.address.postal_code",
    "payment", "metadata"
)
```

#### Step 2: Explode `items`

```python
from pyspark.sql.functions import explode_outer
df = df.withColumn("items", explode_outer("items"))
```

**Result:** 1 order → 2 rows (one per item).

#### Step 3: Flatten items + nested structs

```python
df = df.select(
    "items.item_id", "items.price", "items.product_name", "items.quantity",
    "order_id", "timestamp",
    "customer_id", "name", "email", "city", "country", "postal_code",
    "payment.method", "payment.transaction_id",
    "metadata"
)
```

#### Step 4: Explode `metadata`

```python
df = df.withColumn("metadata", explode_outer("metadata"))
df = df.select("*", "metadata.key", "metadata.value").drop("metadata")
```

**Result:** Each previous row is multiplied by the number of metadata records. For the sample order: 2 items × 2 metadata = **4 rows**.

### 4.3 Writing the Output to Delta Lake

After the transformations are complete, the DataFrame is written to a Delta table using `writeStream`.

```python
df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .trigger(once=True) \
    .option("path", "/Volumes/databricks_streaming/stream/streaming/jsonsink/Data") \
    .option("checkpointLocation", "/Volumes/databricks_streaming/stream/streaming/jsonsink/checkpoint") \
    .start()
```

#### Option Breakdown

| Option | Value | Purpose |
| :--- | :--- | :--- |
| `format` | `"delta"` | Write as a Delta table (ACID, versioned) |
| `outputMode` | `"append"` | Append-only; no updates or overwrites |
| `trigger` | `once=True` | Process the current batch and stop |
| `path` | `.../jsonsink/Data` | Target location for Delta files |
| `checkpointLocation` | `.../jsonsink/checkpoint` | Stores stream progress and schema state |

#### Why Each Option Matters

**`format("delta")`**
- Provides ACID transactions, schema enforcement, and time travel.

**`outputMode("append")`**
- Correct mode for immutable data (no updates or deletes).

**`trigger(once=True)`**
- Batch-style execution: processes all available data once, then stops.

**`checkpointLocation`**
- **Critical for streaming.** Stores:
  - Which files have been processed.
  - Schema state per micro-batch.
- **If deleted, the stream reprocesses all source files.**

#### Output Structure

```
/Volumes/databricks_streaming/stream/streaming/jsonsink/
├── Data/            ← Delta table files
└── checkpoint/      ← Stream progress and schema state
```

Query the output:

```sql
SELECT * FROM delta.`/Volumes/databricks_streaming/stream/streaming/jsonsink/Data` LIMIT 10;
```

### 4.4 Archiving Source Files

`spark.readStream` does **not** maintain a checkpoint for "which files have been processed" the way Auto Loader does. To avoid reprocessing files on subsequent runs, source files are **moved out** after processing. This is called **archiving**.

#### Directory Layout

| Directory | Role |
| :--- | :--- |
| `jsonsourcenew/` | **Input** — files to be read by `readStream` |
| `jsonsourcearchive/` | **Archive** — processed files are moved here |
| `jsonsinknew/Data/` | **Output** — Delta table written by `writeStream` |

#### Code

```python
df = (spark.readStream
  .format("json")
  .option("multiLine", True)
  .schema(my_schema)
  .option("cleanSource", "archive")
  .option("sourceArchiveDir", "/Volumes/.../jsonsourcearchive")
  .load("/Volumes/.../jsonsourcenew")
)

df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .trigger(once=True) \
  .option("path", "/Volumes/.../jsonsinknew/Data") \
  .option("checkpointLocation", "/Volumes/.../jsonsinknew/checkpoint") \
  .start()
```

| Option | Value | Purpose |
| :--- | :--- | :--- |
| `cleanSource` | `"archive"` | After processing, move source files to the archive directory |
| `sourceArchiveDir` | `.../jsonsourcearchive` | Target directory for archived files |

#### Observed Behavior (step-by-step)

| Step | Action | After Run: `jsonsourcenew/` | After Run: `jsonsourcearchive/` | After Run: `jsonsinknew/Data` |
| :--- | :--- | :--- | :--- | :--- |
| 1 | Upload `day1`, run | day1 (not archived) | (empty) | day1 data |
| 2 | Upload `day2`, run | day2 | day1 | day1 + day2 data |
| 3 | Re-upload `day1`, run | day1, day2 | day1 | day1 + day2 data (with duplicates) |
| 4 | Upload `day3`, run | day1, day3 | day1, day2 | day1 + day2 + day3 data |
| 5 | Run again (no new file) | day1, day3 | day1, day2 | (unchanged) |

**Key observations:**
- **The latest uploaded file stays in `jsonsourcenew/`** until the next new file arrives.
- **The previous "latest" file is archived** when a newer file comes in.
- **Re-uploading `day1`** produces duplicate rows in the output (checkpoint only tracks paths + timestamps, not content).
- **Running again with no new file** does not change anything.

**Conclusion:** The archive mechanism follows a **"retain the latest, archive the rest"** pattern. The input directory is never fully emptied — it always holds the most recent file waiting for the next one.

#### Comparison: Archive vs. Auto Loader

| Approach | Prevents Reprocessing? | Schema Evolution? | Retains Latest File? |
| :--- | :--- | :--- | :--- |
| `readStream` + archive | ✅ (via file movement) | ❌ | ✅ |
| Auto Loader (`cloudFiles`) | ✅ (via checkpoint) | ✅ | ❌ |

Auto Loader is the recommended modern approach. The archive pattern is shown here for completeness, since some legacy pipelines use it.

### 4.5 Output Modes

`outputMode` controls how a streaming DataFrame writes its results to the sink.

| Mode | Output Behavior | Destination State |
| :--- | :--- | :--- |
| `append` | Only new rows are written. No updates or overwrites. | Grows over time; no updates |
| `update` | Only rows whose values changed in the latest batch are written. | Same final state as `complete` |
| `complete` | The **entire result set** is rewritten on every batch. | Same final state as `update` |

**Key distinction:**
- `update` and `complete` produce the **same final destination state**.
- The difference is **what gets written per batch**:
  - `complete` → rewrites the entire result set each time.
  - `update` → writes only the rows that changed in the latest batch.

**Note:** Aggregations (`groupBy`) cannot use `append` mode — they must use `update` or `complete`.

#### Creating the Source Table

A small table is used as the streaming source:

```sql
CREATE TABLE IF NOT EXISTS databricks_streaming.stream.sourcetable (
    color STRING
);

INSERT INTO databricks_streaming.stream.sourcetable VALUES
    ('red'),
    ('green'),
    ('blue'),
    ('yellow'),
    ('orange'),
    ('orange');
```

#### Running with `complete` Mode

The aggregation groups by `color` and counts occurrences:

```python
df = spark.readStream.table("databricks_streaming.stream.sourcetable")

df = df.groupBy("color").agg(count("*").alias("count"))

df.writeStream.format("delta") \
    .outputMode("complete") \
    .trigger(once=True) \
    .option("checkpointLocation", "/Volumes/databricks_streaming/stream/streaming/output/check") \
    .option("path", "/Volumes/databricks_streaming/stream/streaming/output/Data") \
    .start()
```

#### Simulating Multiple Batches

To observe how `complete` mode behaves, three insert batches were applied:

| Batch | Inserted | After Run: Aggregated Result |
| :--- | :--- | :--- |
| 1 | red, green, blue, yellow, orange, orange | red:2, green:2, blue:2, yellow:1, orange:2 |
| 2 | red, green, blue | red:2, green:2, blue:2, yellow:1, orange:2 |
| 3 | maroon | red:2, green:2, blue:2, yellow:1, orange:2, maroon:1 |

#### Observed Result

```sql
SELECT * FROM delta.`/Volumes/databricks_streaming/stream/streaming/output/Data`;
```

| color | count |
| :--- | :--- |
| yellow | 1 |
| orange | 2 |
| maroon | 1 |
| green | 2 |
| blue | 2 |
| red | 2 |

The output reflects the **full aggregated state** of the source table — confirming `complete` mode behavior.

#### Note: `update` Mode Not Available in Free Edition

The tutorial also demonstrates `update` mode, but **Databricks Free Edition does not support it**.

If it were available:
- **`update` and `complete` would produce the same final destination state.**
- The difference is **what gets written during each batch**:
  - `complete` → rewrites the entire result set every time.
  - `update` → writes only the rows whose values changed in the latest batch.

In the final run (inserting `maroon`):
- `complete` output: the full table (`red:2, green:2, blue:2, yellow:1, orange:2, maroon:1`).
- `update` output (hypothetical): only `maroon:1` — the row that changed.

#### Summary

| Mode | Final Destination | Per-Batch Output |
| :--- | :--- | :--- |
| `append` | Grows over time | New rows only |
| `update` | Full aggregated state | Only changed rows |
| `complete` | Full aggregated state | Entire result set (rewritten) |

### 4.6 foreachBatch: Multiple Sinks from One Stream

`foreachBatch` allows custom logic to be applied to each micro-batch of a streaming DataFrame. It is commonly used to:
- Write to **multiple sinks** in one pass.
- Perform operations not supported by native streaming sinks (e.g., `MERGE`, `upsert`).
- Apply custom Python logic per batch.

#### Full Code

```python
# 1. Define the per-batch function
def myfunc(df, batch_id):
    df = df.groupBy("color").agg(count("*").alias("count"))

    # Destination 1
    df.write.format("delta").mode("append") \
        .option("path", ".../foreachsink/dest1").save()

    # Destination 2
    df.write.format("delta").mode("append") \
        .option("path", ".../foreachsink/dest2").save()


# 2. Start the stream, calling myfunc per batch
df.writeStream.foreachBatch(myfunc) \
    .outputMode("append") \
    .trigger(once=True) \
    .option("checkpointLocation", ".../foreachsink/checkpoint") \
    .start()
```

#### Key Points

| Element | Purpose |
| :--- | :--- |
| `myfunc(df, batch_id)` | Custom logic for each batch |
| `df` | The current micro-batch (not the full stream) |
| `batch_id` | Batch sequence number (0, 1, 2, ...) |
| `foreachBatch(myfunc)` | Applies `myfunc` to each batch |
| `outputMode("append")` | How the outer stream feeds batches |
| `checkpointLocation` | Required — stores stream progress |

#### Why Not Just Use `writeStream` Directly?

A single `writeStream` writes to **only one sink**. To write the same data to multiple destinations, use `foreachBatch`:

| Approach | Number of Sinks |
| :--- | :--- |
| `writeStream.format("delta").option("path", "sink1").start()` | 1 |
| `foreachBatch` with multiple `.write` calls | 2+ |

#### Note: Same Content in Both Sinks (by Design)

In the tutorial, `dest1` and `dest2` receive identical content because both are written from the same `df`. This is intentional for demonstration — the goal is to show that `foreachBatch` can write to **multiple sinks in one pass**.

In production, the two sinks typically differ:
- **Hot vs. cold**: Delta (for BI) + Parquet (for archival)
- **Detail vs. aggregate**: full records + aggregated summary
- **Different systems**: Delta + Kafka / JDBC / external APIs

Example: writing a detail table and an aggregated summary:

```python
def myfunc(df, batch_id):
    # dest1: full detail
    df.write.format("delta").mode("append").option("path", ".../dest1").save()

    # dest2: aggregated summary
    df.groupBy("color").agg(count("*").alias("count")) \
        .write.format("delta").mode("append").option("path", ".../dest2").save()
```

### 4.7 Windowed Aggregation

Streaming allows **time-windowed aggregations** using the `window()` function. This groups events into fixed time intervals (e.g., every 10 minutes) and aggregates them.

#### Creating the Source Table

```sql
CREATE TABLE IF NOT EXISTS databricks_streaming.stream.windowtbl (
    color STRING,
    event_date TIMESTAMP
);

INSERT INTO databricks_streaming.stream.windowtbl
VALUES ('red', '2025-01-01T11:07:00.000+00:00');
```

#### Windowed Aggregation

```python
from pyspark.sql.functions import window, count, lit

df = spark.readStream.table("databricks_streaming.stream.windowtbl")

df = df.groupBy(
    "color",
    window("event_date", "10 minutes")
).agg(count(lit(1)).alias("color_count"))

df.writeStream.format("delta") \
    .outputMode("complete") \
    .trigger(once=True) \
    .option("path", ".../windows/Data") \
    .option("checkpointLocation", ".../windows/checkpoint") \
    .start()
```

#### How `window()` Works

`window("event_date", "10 minutes")` splits the timeline into 10-minute buckets:

| Window | Time Range | Events Included |
| :--- | :--- | :--- |
| Window 1 | 11:00 – 11:10 | red (11:07) |
| Window 2 | 11:10 – 11:20 | — |
| Window 3 | 11:20 – 11:30 | — |

Each event is assigned to the window that contains its timestamp. Aggregation (`count`) is then applied per window.

#### Why `complete` Mode?

Windowed aggregations produce results that change as time progresses. `complete` mode rewrites the full result set on each batch, ensuring the latest window state is always visible.

#### Result

```sql
SELECT * FROM delta.`.../windows/Data`;
```

| color | window | color_count |
| :--- | :--- | :--- |
| red | 2025-01-01 11:00:00 – 11:10:00 | 1 |

#### Summary

| Concept | Meaning |
| :--- | :--- |
| `window(col, duration)` | Split timeline into fixed intervals |
| `groupBy("color", window(...))` | Aggregate per color per window |
| `outputMode("complete")` | Rewrite the full window result each batch |

#### Event Time vs. Processing Time

In windowed aggregations, the time dimension is usually **Event Time** — the timestamp recorded inside the data — not **Processing Time** — when Spark processes it.

| Concept | Meaning | Example |
| :--- | :--- | :--- |
| **Event Time** | When the event actually happened | `2025-01-01 11:07` (from `event_date`) |
| **Processing Time** | When Spark processed the event | `2025-01-01 11:15` (arrival time) |

**Why Event Time Matters:**
- Business questions are about **when things happened**, not when they arrived.
- Example: "Sales in the 11:00–11:10 window" should include orders placed at 11:07, even if they arrive late.
- Using Processing Time would incorrectly bucket late-arriving data into the wrong window.

**Out-of-order Data:**
Real-world data rarely arrives in Event Time order. A network delay can cause an 11:07 event to arrive at 11:15. Structured Streaming handles this via **Watermarks** — a time threshold that decides when a window can be safely closed.

**In this project:** `window("event_date", "10 minutes")` uses `event_date` (Event Time) to bucket events, ensuring the aggregation is business-correct.

---

## 5. Observation: Row Multiplication from Multiple Explodes

Exploding two arrays on the same DataFrame produces a Cartesian product:

| Step | Rows (for the sample) |
| :--- | :--- |
| Original | 1 |
| After `explode(items)` | 2 |
| After `explode(metadata)` | 4 |

**In production**, this can cause significant data expansion:
- 1M orders × 3 items × 4 metadata = **12M rows**

**Mitigation strategies:**
- **Explode only the array that defines the analysis granularity** (usually `items`).
- **Keep secondary arrays (e.g., `metadata`) un-exploded** and process them in a separate table joined by `order_id`.

This project **intentionally follows the reference tutorial** and explodes both arrays for demonstration purposes. The row-multiplication trade-off is acknowledged as acceptable for the small sample dataset.

---

## 6. Summary

| Step | Action | Why |
| :--- | :--- | :--- |
| 1 | Batch read JSON | Infer structure |
| 2 | `printSchema()` | See nesting and types |
| 3 | Generate DDL (or reuse schema) | For streaming reader |
| 4 | `select` top-level + nested | Prepare for explode |
| 5 | `explode(items)` | One item per row |
| 6 | Flatten nested into columns | Produce flat Silver table |
| 7 | `explode(metadata)` | One key-value per row |
| 8 | Acknowledge row multiplication | Trade-off for small data |

---

## 7. References

- **JSON Formatter (visual inspection):** [https://jsonformatter.org/](https://jsonformatter.org/)
- **Reference Tutorial:** [https://www.youtube.com/watch?v=r7FTCuTl84g&t=5291s](https://www.youtube.com/watch?v=r7FTCuTl84g&t=5291s)
