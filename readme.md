# JSON Streaming with Databricks

This project covers two parts:

- **Part 1:** Ingest and flatten nested JSON order data (day1 / day2 / day3).
- **Part 2:** Demonstrate core streaming concepts (output modes, `foreachBatch`, windowing) using a small `color` dataset.

---

# Part 1: JSON Order Ingestion

## 1.1 Source Data: One Order per JSON Record

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

## 1.2 Inspection Workflow

For nested JSON, do not write the whole transformation in one go. Inspect each step.

### Batch read

```python
df = (spark.read
  .format("json")
  .option("multiLine", True)
  .load("/Volumes/.../jsonsource")
)
display(df)
```

### Print schema

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

## 1.3 Generating the DDL for Streaming

Streaming readers use an explicit schema.

### Reuse the inferred schema object

```python
my_schema = df.schema
df_stream = spark.readStream.format("json") \
    .option("multiLine", True) \
    .schema(my_schema) \
    .load("/Volumes/.../jsonsource")
```

### Or write a DDL string

There is **no built-in API** to generate a multi-line DDL. It must be hand-written or AI-generated from `printSchema()`.

**Conversion rules:**

| Python type | DDL keyword |
| :--- | :--- |
| `StringType()` | `STRING` |
| `LongType()` | `BIGINT` |
| `DoubleType()` | `DOUBLE` |
| `StructType([...])` | `STRUCT<...>` |
| `ArrayType(StructType([...]))` | `ARRAY<STRUCT<...>>` |

**Syntax warning:** Field name and type are separated by **space**, not colon.
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

JSON uses two container types:

| Symbol | Type | Spark Handling |
| :--- | :--- | :--- |
| `{ }` | Object (struct) | Access with `.` → `col("a.b")` |
| `[ ]` | Array (list) | Explode with `explode()` |
| No bracket | Scalar | Access with `col("a")` |

**In this project:**
- `customer`, `customer.address`, `payment` → structs → access with `.`
- `items`, `metadata` → arrays → explode

## 1.4 Transformation Workflow

### Step 1: Select fields

```python
df = df.select(
    "items", "order_id", "timestamp",
    "customer.customer_id", "customer.name", "customer.email",
    "customer.address.city", "customer.address.country", "customer.address.postal_code",
    "payment", "metadata"
)
```

### Step 2: Explode `items`

```python
from pyspark.sql.functions import explode_outer
df = df.withColumn("items", explode_outer("items"))
```

**Result:** 1 order → 2 rows (one per item).

### Step 3: Flatten items + nested structs

```python
df = df.select(
    "items.item_id", "items.price", "items.product_name", "items.quantity",
    "order_id", "timestamp",
    "customer_id", "name", "email", "city", "country", "postal_code",
    "payment.method", "payment.transaction_id",
    "metadata"
)
```

### Step 4: Explode `metadata`

```python
df = df.withColumn("metadata", explode_outer("metadata"))
df = df.select("*", "metadata.key", "metadata.value").drop("metadata")
```

**Result:** Each previous row is multiplied by the number of metadata records. For the sample order: 2 items × 2 metadata = **4 rows**.

## 1.5 Writing to Delta

```python
df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .trigger(once=True) \
    .option("path", "/Volumes/.../jsonsink/Data") \
    .option("checkpointLocation", "/Volumes/.../jsonsink/checkpoint") \
    .start()
```

| Option | Value | Purpose |
| :--- | :--- | :--- |
| `format` | `"delta"` | Delta table (ACID, versioned) |
| `outputMode` | `"append"` | Append-only |
| `trigger` | `once=True` | Process once, then stop |
| `path` | `.../jsonsink/Data` | Delta output location |
| `checkpointLocation` | `.../jsonsink/checkpoint` | Stream progress |

**Output structure:**

```
/Volumes/.../jsonsink/
├── Data/            ← Delta files
└── checkpoint/      ← Stream progress and schema state
```

Query:

```sql
SELECT * FROM delta.`/Volumes/.../jsonsink/Data` LIMIT 10;
```

## 1.6 Archiving Source Files

`spark.readStream` does not maintain a checkpoint of processed files. To avoid reprocessing, processed files are moved to an archive directory.

```python
df = (spark.readStream
  .format("json")
  .option("multiLine", True)
  .schema(my_schema)
  .option("cleanSource", "archive")
  .option("sourceArchiveDir", "/Volumes/.../jsonsourcearchive")
  .load("/Volumes/.../jsonsourcenew")
)
```

| Option | Value | Purpose |
| :--- | :--- | :--- |
| `cleanSource` | `"archive"` | Move processed files |
| `sourceArchiveDir` | `.../jsonsourcearchive` | Archive target |

### Observed behavior (step-by-step)

| Step | Action | `jsonsourcenew/` | `jsonsourcearchive/` | `jsonsinknew/Data` |
| :--- | :--- | :--- | :--- | :--- |
| 1 | Upload `day1`, run | day1 | (empty) | day1 data |
| 2 | Upload `day2`, run | day2 | day1 | day1 + day2 |
| 3 | Re-upload `day1`, run | day1, day2 | day1 | day1 + day2 (duplicates) |
| 4 | Upload `day3`, run | day1, day3 | day1, day2 | day1 + day2 + day3 |
| 5 | Run again (no new file) | day1, day3 | day1, day2 | (unchanged) |

**Key observations:**
- The latest uploaded file stays in `jsonsourcenew/` until the next new file arrives.
- The previous "latest" file is archived.
- Re-uploading `day1` produces duplicate rows (checkpoint tracks path + timestamp, not content).

**Conclusion:** The archive mechanism follows a **"retain the latest, archive the rest"** pattern.

**Comparison:**

| Approach | Prevents Reprocessing? | Schema Evolution? | Retains Latest? |
| :--- | :--- | :--- | :--- |
| `readStream` + archive | ✅ | ❌ | ✅ |
| Auto Loader (`cloudFiles`) | ✅ | ✅ | ❌ |

## 1.7 Row Multiplication from Multiple Explodes

Exploding two arrays produces a Cartesian product:

| Step | Rows (for the sample order) |
| :--- | :--- |
| Original | 1 |
| After `explode(items)` | 2 |
| After `explode(metadata)` | 4 |

**In production:** 1M orders × 3 items × 4 metadata = **12M rows**.

**Mitigation:**
- Explode only the array that defines the analysis granularity (usually `items`).
- Keep secondary arrays (e.g., `metadata`) un-exploded; process separately joined by `order_id`.

This project intentionally follows the reference tutorial and explodes both arrays for demonstration.

---

# Part 2: Streaming Concepts (using color data)

The following sections use a minimal `color` dataset (`red, green, blue, yellow, orange, maroon`) to demonstrate streaming behaviors clearly.

## 2.1 Output Modes

`outputMode` controls how a streaming DataFrame writes results.

| Mode | Output Behavior | Destination State |
| :--- | :--- | :--- |
| `append` | Only new rows are written. | Grows over time |
| `update` | Only rows whose values changed in the latest batch. | Same final state as `complete` |
| `complete` | The entire result set is rewritten on every batch. | Same final state as `update` |

**Key distinction:** `update` and `complete` produce the same final destination state; they differ in what gets written per batch.

**Aggregations (`groupBy`) cannot use `append`.**

### Source table

```sql
CREATE TABLE IF NOT EXISTS databricks_streaming.stream.sourcetable (
    color STRING
);

INSERT INTO databricks_streaming.stream.sourcetable VALUES
    ('red'), ('green'), ('blue'), ('yellow'), ('orange'), ('orange');
```

### `complete` mode example

```python
df = spark.readStream.table("databricks_streaming.stream.sourcetable")

df = df.groupBy("color").agg(count("*").alias("count"))

df.writeStream.format("delta") \
    .outputMode("complete") \
    .trigger(once=True) \
    .option("checkpointLocation", ".../output/check") \
    .option("path", ".../output/Data") \
    .start()
```

### Simulating three batches

| Batch | Inserted | Aggregated Result |
| :--- | :--- | :--- |
| 1 | red, green, blue, yellow, orange, orange | red:2, green:2, blue:2, yellow:1, orange:2 |
| 2 | red, green, blue | red:2, green:2, blue:2, yellow:1, orange:2 |
| 3 | maroon | red:2, green:2, blue:2, yellow:1, orange:2, maroon:1 |

### Observed result

| color | count |
| :--- | :--- |
| yellow | 1 |
| orange | 2 |
| maroon | 1 |
| green | 2 |
| blue | 2 |
| red | 2 |

### Note: `update` mode not available in Free Edition

If available, `update` and `complete` would produce the same final state. The difference:
- `complete` → rewrites the entire result set per batch.
- `update` → writes only rows whose values changed in the latest batch (e.g., only `maroon:1` in the final run).

## 2.2 foreachBatch: Multiple Sinks

`foreachBatch` applies custom logic to each micro-batch. Common uses:
- Write to multiple sinks in one pass.
- Perform `MERGE` / `upsert` (not supported by native streaming sinks).
- Apply custom Python logic per batch.

```python
def myfunc(df, batch_id):
    df = df.groupBy("color").agg(count("*").alias("count"))

    # Destination 1
    df.write.format("delta").mode("append") \
        .option("path", ".../dest1").save()

    # Destination 2
    df.write.format("delta").mode("append") \
        .option("path", ".../dest2").save()


df.writeStream.foreachBatch(myfunc) \
    .outputMode("append") \
    .trigger(once=True) \
    .option("checkpointLocation", ".../foreachsink/checkpoint") \
    .start()
```

| Element | Purpose |
| :--- | :--- |
| `myfunc(df, batch_id)` | Custom logic per batch |
| `df` | The current micro-batch |
| `batch_id` | Batch sequence number |
| `foreachBatch(myfunc)` | Apply function to each batch |
| `checkpointLocation` | Required |

**In the tutorial, both sinks receive identical content** — this is by design, to demonstrate the ability to write to multiple sinks in one pass. In production, sinks typically differ (hot vs. cold, detail vs. aggregate, Delta vs. Kafka).

## 2.3 Windowed Aggregation

Streaming supports time-windowed aggregations via `window()`.

### Source table

```sql
CREATE TABLE IF NOT EXISTS databricks_streaming.stream.windowtbl (
    color STRING,
    event_date TIMESTAMP
);
```

### Test data (three batches)

```sql
-- Batch 1
INSERT INTO databricks_streaming.stream.windowtbl VALUES
    ('red',   '2025-01-01T11:01:00.000+00:00'),
    ('green', '2025-01-01T11:01:00.000+00:00');

-- Batch 2
INSERT INTO databricks_streaming.stream.windowtbl VALUES
    ('green', '2025-01-01T11:07:00.000+00:00');

-- Batch 3
INSERT INTO databricks_streaming.stream.windowtbl VALUES
    ('green', '2025-01-01T11:12:00.000+00:00');
```

| color | event_date |
| :--- | :--- |
| red | 2025-01-01 11:01:00 |
| green | 2025-01-01 11:01:00 |
| green | 2025-01-01 11:07:00 |
| green | 2025-01-01 11:12:00 |

### Windowed aggregation

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

### Observed result

| color | window | color_count |
| :--- | :--- | :--- |
| green | 2025-01-01 11:10:00 – 11:20:00 | 1 |
| green | 2025-01-01 11:00:00 – 11:10:00 | 2 |
| red | 2025-01-01 11:00:00 – 11:10:00 | 1 |

| Window | Events | Aggregation |
| :--- | :--- | :--- |
| 11:00 – 11:10 | red (11:01), green (11:01), green (11:07) | red: 1, green: 2 |
| 11:10 – 11:20 | green (11:12) | green: 1 |

The `window` column is a struct with `start` and `end` fields.

## 2.4 Event Time vs. Processing Time

| Concept | Meaning |
| :--- | :--- |
| **Event Time** | When the event actually happened |
| **Processing Time** | When Spark processed the event |

**Windowed aggregations use Event Time**, not Processing Time, so that late-arriving events are still bucketed into the correct business window.

Real-world data rarely arrives in order. Structured Streaming uses **Watermarks** to handle out-of-order data — a threshold that determines when a window can be closed.

#### Watermark: Handling Late Data

Real-world data rarely arrives in Event Time order. Network delays, retries, or upstream buffering can cause an event with timestamp `11:07` to arrive at `11:15`. Without a mechanism to handle this, streaming windows would need to wait indefinitely for late data.

A **Watermark** is a time threshold that tells Spark when it can safely stop waiting for late events.

**How it works:**
- Spark tracks the maximum Event Time seen so far (e.g., `11:25`).
- The Watermark is computed as `max_event_time - watermark_delay` (e.g., `11:25 - 5min = 11:20`).
- Any event with a timestamp **earlier than the Watermark** is considered "too late" and is dropped or ignored.
- Windows whose end time is **before the Watermark** are closed and their state is cleared.

**Example (10-minute window, 5-minute watermark):**

| Event Arrives | Event Time | Latest Event Time | Watermark | Window Status |
| :--- | :--- | :--- | :--- | :--- |
| Event 1 | 11:05 | 11:05 | 11:00 | 11:00–11:10 still open |
| Event 2 | 11:15 | 11:15 | 11:10 | 11:00–11:10 still waiting |
| Event 3 | 11:25 | 11:25 | 11:20 | 11:00–11:10 closed |
| Event 4 | 11:08 | 11:25 | 11:20 | **Dropped** (too late) |

**Key points:**
- **Watermark ≠ a larger window.** It is a *delay tolerance*, not a time bucket.
- **Window** decides which bucket an event belongs to.
- **Watermark** decides when a bucket can be closed and its state cleared.

**Not used in this project:** Since all streaming in this project runs with `trigger(once=True)` (batch-style), Watermark is not required. It becomes essential in continuous streaming pipelines that process late-arriving data.

## 2.5 Window Types

| Type | Syntax | Overlap |
| :--- | :--- | :--- |
| **Tumbling** | `window(col, "10 minutes")` | ❌ No overlap |
| **Sliding** | `window(col, "10 minutes", "5 minutes")` | ✅ Overlaps by slide interval |
| **Session** | `session_window(col, "10 minutes")` | Based on activity gaps |

**Tumbling (used in this project):** Fixed 10-minute buckets, no overlap. Each event belongs to exactly one window.

**Sliding:** Fixed window length, but a slide interval smaller than the window length causes overlap. Each event can belong to multiple windows. Use case: "rolling last-10-minute aggregation updated every 5 minutes."

**Session:** Windows are defined by user activity gaps. A session closes after a period of inactivity. Use case: "how many pages did a user visit in one session?"

---

## Summary

| Section | Concept |
| :--- | :--- |
| Part 1 | JSON ingestion, DDL, explode, archive |
| Part 2.1 | Output modes (`append` / `update` / `complete`) |
| Part 2.2 | `foreachBatch` for multiple sinks |
| Part 2.3 | Windowed aggregation |
| Part 2.4 | Event Time vs. Processing Time |
| Part 2.5 | Tumbling / Sliding / Session windows |

---

## References

- **JSON Formatter:** [https://jsonformatter.org/](https://jsonformatter.org/)
- **Reference Tutorial:** [https://www.youtube.com/watch?v=r7FTCuTl84g&t=5291s](https://www.youtube.com/watch?v=r7FTCuTl84g&t=5291s)