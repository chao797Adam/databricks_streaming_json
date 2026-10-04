
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
