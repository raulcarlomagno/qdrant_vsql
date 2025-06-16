# Qdrant VSQL

![Python Version](https://img.shields.io/badge/python-3.9%2B-blue)
![License](https://img.shields.io/badge/license-GPLv3-blue)
![PyPI Version](https://img.shields.io/pypi/v/qdrant-vsql.svg)

## 🚀 Overview

Qdrant VSQL (VectorSQL) is an ambitious Python library aiming to bring a SQL-like query interface to Qdrant vector databases. Currently, it provides a robust solution for converting SQL-like `WHERE` clauses into [Qdrant `Filter`](https://qdrant.tech/documentation/concepts/filtering/) objects, simplifying complex filtering logic.

Maybe Qdrant VSQL will evolve into a comprehensive SQL interface for Qdrant, potentially becoming a standard for vector database interactions. Imagine writing intuitive SQL queries for all your Qdrant operations, from filtering to vector similarity search, aggregation, and more!

## ✨ Current Features (Filtering)

- **SQL-like Syntax**: Write familiar `WHERE` clauses (e.g., `age > 30 AND city = 'New York'`).
- **Comprehensive Operator Support**: Includes `=`, `!=`, `<>`, `>`, `>=`, `<`, `<=`, `IN`, `NOT IN`, `BETWEEN`, `NOT BETWEEN`, `LIKE`, `IS NULL`, `IS NOT NULL`, `IS EMPTY`, `COUNT()`.
- **Logical Operators**: Supports `AND`, `OR`, and `NOT` for complex query combinations. Supports intricate logical structures with nested operations. See more examples in the [test suite](tests/test_filtering.py).
- **Nested Field Support**: Easily filter on nested payload fields using dot notation (e.g., `user.address.city`).
- **Array Projection**: Handle array fields with `[]` notation (e.g., `tags[] = 'urgent'`).
- **Type Handling**: Automatically converts string, number, and boolean values to appropriate Qdrant types.
- **Qdrant Native Output**: Generates [`qdrant_client.http.models.Filter`](https://python-client.qdrant.tech/qdrant_client.http.models.models#qdrant_client.http.models.models.Filter) objects, ready for use with the Qdrant client.
- **ID Filtering**: Automatically translates all `id` field operations (e.g., `id = 123`) into Qdrant's native [`HasIdCondition`](https://qdrant.tech/documentation/concepts/filtering/#has-id) for efficient point ID filtering.
- **LIKE Operator**: When using the `LIKE` operator, it automatically translates to Qdrant's [`MatchText`](https://qdrant.tech/documentation/concepts/filtering/#full-text-match) condition for full-text search capabilities. Otherwise it uses [`MatchValue`](https://qdrant.tech/documentation/concepts/filtering/#match) for strings.
- **Case-Insensitive**: Operators and keywords are case-insensitive for flexible query writing.


##  Installation

You can install Qdrant VSQL directly from PyPI:

```bash
pip install qdrant-vsql
```

## 💡 Usage Examples (Current Filtering)

Here's how you can use Qdrant VSQL to build your Qdrant filter queries:

```python
from qdrant_client import QdrantClient
from qdrant_vsql.filtering import where2filter

# Initialize Qdrant client (replace with your actual client setup)
qdrant_client = QdrantClient(":memory:")

# Example 1: Simple AND condition
query_str_1 = "city = 'London' AND age > 30"
qdrant_filter_1 = where2filter(query_str_1)

# Example 2: OR and IN conditions
query_str_2 = "status IN ('active', 'pending') OR priority = 'high'"
qdrant_filter_2 = where2filter(query_str_2)

# Example 3: NOT and BETWEEN conditions
query_str_3 = "NOT (price BETWEEN 100 AND 200) AND category IS NOT NULL"
qdrant_filter_3 = where2filter(query_str_3)

# Example 4: COUNT and LIKE conditions
query_str_4 = "COUNT(tags) >= 2 AND description LIKE 'product'"
qdrant_filter_4 = where2filter(query_str_4)

# Example 5: Datetime range condition
query_str_5 = "event_date BETWEEN '2023-01-01T00:00:00Z' AND '2023-12-31T23:59:59Z'"
qdrant_filter_5 = where2filter(query_str_5)

# Call Qdrant with your filter
hits = client.query_points(
    collection_name="my_collection",
    query=np.random.rand(100),
    query_filter=qdrant_filter_1,
    limit=10
)
```

## 🗺️ SQL to Qdrant Filter Equivalences

This section details the equivalences between common SQL-like `WHERE` clause syntax and the corresponding Qdrant `Filter` object structures. The `where2filter` function translates these SQL expressions into Qdrant's native filtering language, enabling powerful and flexible queries.

A Qdrant `Filter` object typically consists of `must`, `should`, and `must_not` arrays, which represent `AND`, `OR`, and `NOT` logical operations, respectively. Conditions within these arrays are combined to form complex queries.

| SQL-like Syntax | Qdrant Filter Object Equivalent (simplified) | Notes |
|---|---|---|
| `field = 'value'` | `{"key": "field", "match": {"value": "value"}}` | Exact match for strings, numbers, booleans. |
| `field != 'value'` or `field <> 'value'` | `must_not: [{"key": "field", "match": {"value": "value"}}]` | Negation of exact match. |
| `field IN ('val1', 'val2')` | `{"key": "field", "match": {"any": ["val1", "val2"]}}` | Matches if `field` is any of the values. |
| `field NOT IN ('val1', 'val2')` | `must_not: [{"key": "field", "match": {"any": ["val1", "val2"]}}]` | Matches if `field` is none of the values. |
| `field BETWEEN val1 AND val2` | `{"key": "field", "range": {"gte": val1, "lte": val2}}` | Inclusive range for numbers. |
| `field NOT BETWEEN val1 AND val2` | `must_not: [{"key": "field", "range": {"gte": val1, "lte": val2}}]` | Negation of inclusive range. |
| `field > value` | `{"key": "field", "range": {"gt": value}}` | Greater than. |
| `field >= value` | `{"key": "field", "range": {"gte": value}}` | Greater than or equal. |
| `field < value` | `{"key": "field", "range": {"lt": value}}` | Less than. |
| `field <= value` | `{"key": "field", "range": {"lte": value}}` | Less than or equal. |
| `field LIKE 'pattern'` | `{"key": "field", "match": {"text": "pattern%"}}` | Full-text search. |
| `field IS NULL` | `{"is_null": {"key": "field"}}` | Matches if `field` is `null` or does not exist. |
| `field IS NOT NULL` | `must_not: [{"is_null": {"key": "field"}}]` | Matches if `field` is not `null` and exists. |
| `field IS EMPTY` or `field = []` | `{"is_empty": {"key": "field"}}` | Matches if `field` is missing, `null`, or an empty array. |
| `COUNT(field) >= value` | `{"key": "field", "values_count": {"gte": value}}` | Filters by the number of values in an array field. |
| `parent.child = 'value'` | `{"key": "parent.child", "match": {"value": "value"}}` | Accesses nested payload fields using dot notation. |
| `array[].field = 'value'` | `{"key": "array[].field", "match": {"value": "value"}}` | Filters on elements within an array of objects. |
| `id = 123` or `id = 'uuid_str'` | `{"has_id": [123]}` or `{"has_id": ["uuid_str"]}` | Filters by point ID (integer or UUID). |
| `date_field >= 'YYYY-MM-DDTHH:MM:SSZ'` | `{"key": "date_field", "range": {"gte": "YYYY-MM-DDTHH:MM:SSZ"}}` | Datetime range filtering. |
| `date_field BETWEEN 'date1' AND 'date2'` | `{"key": "date_field", "range": {"gte": "date1", "lte": "date2"}}` | Inclusive range for datetimes. |
| `(cond1 OR cond2) AND NOT (cond3 OR cond4)` | `must: [should: [cond1, cond2]], must_not: [should: [cond3, cond4]]` | Complex logical combinations using nested filters. |

## 🚧 Pending Features
Implement `HasVectorCondition`, `NestedCondition` and Geo (`GeoBoundingBox`, `GeoRadius` and `GeoPolygon`).

## 🎯 Future Vision

We envision Qdrant VSQL evolving into a full-fledged SQL interface for Qdrant, allowing you to interact with your vector database using familiar SQL syntax for various operations. This idea was first discussed in the [Qdrant GitHub issue #4026](https://github.com/qdrant/qdrant/issues/4026). This could simplify queries for the Qdrant dashboard, Jupyter Notebooks, Advanced Filtering Support in UIs, and more.

Here are some examples of what we aim to support:

### Vector Similarity Search

```sql
SELECT score, vector, id, payload.brand_name
FROM mycollection
WHERE vector LIKE [0.12, 0.1, 0.99, -0.01]
```

### For named vectors:

```sql
SELECT score, vectors.img_vec, id, payload.qty_items
FROM mycollection
WHERE vectors.img_vec LIKE [0, 1.2, -0.2, 0.001]
```

### Filtering Payload with LIMIT and OFFSET

```sql
SELECT score, id, payload.qty_items
FROM mycollection
WHERE vector LIKE [0.1, 0.2, -0.3, 0.11] AND payload.brand_name = 'Nokia'
LIMIT 50
OFFSET 20
```

### Selecting All Payload Fields

```sql
SELECT score, id, payload.*
FROM mycollection
WHERE vector LIKE [0.11, -0.2, 0.3, 0.22] AND payload.brand_name = 'Nokia'
LIMIT 100
```

### Complex Filtering

```sql
SELECT score, id
FROM mycollection
WHERE vector LIKE [0.01, -0.9, 0.11, 0.0]
AND (payload.brand_name IN ('Nokia', 'Alcatel', 'Sony') OR payload.qty_items >= 10)
AND payload.members IS NULL
LIMIT 100
```

### Scroll API with Full-text, Value Count, Range, and Sorting

```sql
SELECT id, payload.*
FROM mycollection
WHERE payload.brand_name MATCH 'cell'
AND COUNT(payload.members) > 100
AND payload.qty_items BETWEEN 10 AND 20
ORDER BY payload.members DESC
LIMIT 100
```

### Geo-Spatial Filtering

```sql
SELECT score, id
FROM mycollection
WHERE vector LIKE [0.01, -0.9, 0.11, 0.0]
AND (
payload.geofield INSIDE RECTANGLE(52.520711, 13.403683, 52.495862, 13.455868)
OR
payload.geofield INSIDE CIRCLE(52.520711, 13.403683, 1000)
OR
payload.geofield OUTSIDE POLYGON([12.444, 54.12], [24.77, 18.222], [99.91, 12.2])
)
```

## 🤝 Contributing

We welcome contributions! If you have suggestions for improvements, new features, or bug fixes, please feel free to:

1.  **Fork** the repository.
2.  **Clone** your forked repository.
3.  **Create a new branch** for your feature or bug fix.
4.  **Make your changes** and ensure tests pass.
5.  **Commit** your changes with a clear message.
6.  **Push** your branch to your forked repository.
7.  **Open a Pull Request** to the `main` branch of this repository.

Please refer to our [`CONTRIBUTING.md`](CONTRIBUTING.md) for more detailed guidelines.

## 📄 License

This project is licensed under the GNU General Public License Version 3 - see the [LICENSE](LICENSE) file for details.

---
Made with ❤️ for the community