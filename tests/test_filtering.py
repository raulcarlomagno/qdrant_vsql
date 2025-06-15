import pytest
from qdrant_client.http import models
from src.qdrant_vsql.filtering import parse_where_to_filter
from datetime import datetime
import uuid


def test_in_or_range():
    query = "color IN ('red','black') OR age >= 17"
    result = parse_where_to_filter(query)
    expected = models.Filter(
        should=[
            models.FieldCondition(
                key="color",
                match=models.MatchAny(any=["red", "black"]),
            ),
            models.FieldCondition(
                key="age",
                range=models.Range(gte=17),
            ),
        ]
    )
    assert result == expected


@pytest.mark.parametrize(
    "query",
    [
        "city = 'London' AND color <> 'red'",
        "city = 'London' AND color != 'red'",
    ],
)
def test_and_not_equal_both_operators(query):
    result = parse_where_to_filter(query)
    expected = models.Filter(
        must=[
            models.FieldCondition(
                key="city",
                match=models.MatchValue(value="London"),
            ),
        ],
        must_not=[
            models.FieldCondition(
                key="color",
                match=models.MatchValue(value="red"),
            ),
        ],
    )
    assert result == expected





def test_and_gt_bool():
    query = "salary > 50000 AND active = TRUE"
    result = parse_where_to_filter(query)
    expected = models.Filter(
        must=[
            models.FieldCondition(key="salary", range=models.Range(gt=50000)),
            models.FieldCondition(key="active", match=models.MatchValue(value=True)),
        ]
    )
    assert result == expected


def test_and_datetime_range():
    dt_gte_val = datetime.fromisoformat("2023-01-01T00:00:00")
    dt_lt_val = datetime.fromisoformat("2024-01-01T00:00:00")
    query = f"created_at >= '{dt_gte_val.isoformat()}' AND created_at < '{dt_lt_val.isoformat()}'"
    result = parse_where_to_filter(query)
    expected = models.Filter(
        must=[
            models.FieldCondition(
                key="created_at",
                range=models.DatetimeRange(gte=dt_gte_val),
            ),
            models.FieldCondition(
                key="created_at",
                range=models.DatetimeRange(lt=dt_lt_val),
            ),
        ]
    )
    assert result == expected


@pytest.mark.parametrize(
    "query, expected_filter",
    [
        (
            "status IN ('pending', 'approved', 'rejected')",
            models.Filter(
                must=[
                    models.FieldCondition(
                        key="status",
                        match=models.MatchAny(any=["pending", "approved", "rejected"]),
                    )
                ]
            ),
        ),
        (
            "category NOT IN ('electronics','furniture')",
            models.Filter(
                must_not=[
                    models.FieldCondition(
                        key="category", match=models.MatchAny(any=["electronics", "furniture"])
                    )
                ]
            ),
        ),
    ],
)
def test_in_list_conditions(query, expected_filter):
    result = parse_where_to_filter(query)
    assert result == expected_filter


@pytest.mark.parametrize(
    "query, expected_filter",
    [
        (
            "price BETWEEN 10 AND 100",
            models.Filter(
                must=[models.FieldCondition(key="price", range=models.Range(gte=10, lte=100))]
            ),
        ),
        (
            "discount NOT BETWEEN 0 AND 0.5",
            models.Filter(
                must_not=[
                    models.FieldCondition(key="discount", range=models.Range(gte=0, lte=0.5))
                ]
            ),
        ),
    ],
)
def test_between_conditions(query, expected_filter):
    result = parse_where_to_filter(query)
    assert result == expected_filter


@pytest.mark.parametrize(
    "query, expected_filter",
    [
        (
            "age = 30",
            models.Filter(
                must=[models.FieldCondition(key="age", match=models.MatchValue(value=30))]
            ),
        ),
        (
            "description = 'A simple text'",
            models.Filter(
                must=[
                    models.FieldCondition(
                        key="description", match=models.MatchValue(value="A simple text")
                    )
                ]
            ),
        ),
        (
            "uuid = 'f47ac10b-58cc-4372-a567-0e02b2c3d479'",
            models.Filter(
                must=[
                    models.FieldCondition(
                        key="uuid", match=models.MatchValue(value="f47ac10b-58cc-4372-a567-0e02b2c3d479")
                    )
                ]
            ),
        ),
    ],
)
def test_simple_equality_conditions(query, expected_filter):
    result = parse_where_to_filter(query)
    assert result == expected_filter


@pytest.mark.parametrize(
    "query, expected_filter",
    [
        (
            "email IS NULL",
            models.Filter(
                must=[models.IsNullCondition(is_null=models.PayloadField(key="email"))]
            ),
        ),
        (
            "phone IS NOT NULL",
            models.Filter(
                must_not=[models.IsNullCondition(is_null=models.PayloadField(key="phone"))]
            ),
        ),
    ],
)
def test_null_conditions(query, expected_filter):
    result = parse_where_to_filter(query)
    assert result == expected_filter


def test_or_and_bool():
    query = "(country = 'US' OR country = 'CA') AND verified = FALSE"
    result = parse_where_to_filter(query)
    expected = models.Filter(
        must=[
            models.Filter(
                should=[
                    models.FieldCondition(
                        key="country", match=models.MatchValue(value="US")
                    ),
                    models.FieldCondition(
                        key="country", match=models.MatchValue(value="CA")
                    ),
                ]
            ),
            models.FieldCondition(key="verified", match=models.MatchValue(value=False)),
        ]
    )
    assert result == expected




@pytest.mark.parametrize(
    "query, expected_filter",
    [
        (
            "COUNT(tags) >= 3",
            models.Filter(
                must=[models.FieldCondition(key="tags", values_count=models.ValuesCount(gte=3))]
            ),
        ),
        (
            "COUNT(comments) BETWEEN 5 AND 20",
            models.Filter(
                must=[
                    models.FieldCondition(
                        key="comments", values_count=models.ValuesCount(gte=5, lte=20)
                    )
                ]
            ),
        ),
    ],
)
def test_count_conditions(query, expected_filter):
    result = parse_where_to_filter(query)
    assert result == expected_filter


def test_nested_field():
    query = "user.address.city = 'London'"
    result = parse_where_to_filter(query)
    expected = models.Filter(
        must=[
            models.FieldCondition(
                key="user.address.city", match=models.MatchValue(value="London")
            )
        ]
    )
    assert result == expected


def test_array_in():
    query = "metadata.tags IN ('urgent','todo')"
    result = parse_where_to_filter(query)
    expected = models.Filter(
        must=[
            models.FieldCondition(
                key="metadata.tags", match=models.MatchAny(any=["urgent", "todo"])
            )
        ]
    )
    assert result == expected


def test_complex_combined():
    query = "((status = 'live' AND views > 1000) OR priority IN ('high','urgent')) AND archived = FALSE AND comments_count BETWEEN 1 AND 10"
    result = parse_where_to_filter(query)
    expected = models.Filter(
        must=[
            models.Filter(
                should=[
                    models.Filter(
                        must=[
                            models.FieldCondition(
                                key="status", match=models.MatchValue(value="live")
                            ),
                            models.FieldCondition(
                                key="views", range=models.Range(gt=1000)
                            ),
                        ]
                    ),
                    models.FieldCondition(
                        key="priority", match=models.MatchAny(any=["high", "urgent"])
                    ),
                ]
            ),
            models.FieldCondition(key="archived", match=models.MatchValue(value=False)),
            models.FieldCondition(
                key="comments_count", range=models.Range(gte=1, lte=10)
            ),
        ]
    )
    assert result == expected


def test_and_float_bool():
    query = "rating >= 4.5 AND published = TRUE"
    result = parse_where_to_filter(query)
    expected = models.Filter(
        must=[
            models.FieldCondition(key="rating", range=models.Range(gte=4.5)),
            models.FieldCondition(key="published", match=models.MatchValue(value=True)),
        ]
    )
    assert result == expected


def test_and_isnotnull_and_lt():
    dt_lt_val = datetime.fromisoformat("2025-04-01T12:00:00")
    query = f"last_login IS NOT NULL AND last_login < '{dt_lt_val.isoformat()}'"
    result = parse_where_to_filter(query)
    expected = models.Filter(
        must=[
            models.FieldCondition(
                key="last_login",
                range=models.DatetimeRange(lt=dt_lt_val),
            ),
        ],
        must_not=[
            models.IsNullCondition(is_null=models.PayloadField(key="last_login"))
        ],
    )
    assert result == expected


def test_empty_string():
    query = "notes = ''"
    result = parse_where_to_filter(query)
    expected = models.Filter(
        must=[models.FieldCondition(key="notes", match=models.MatchValue(value=""))]
    )
    assert result == expected


def test_empty_array():
    query = "attachments = []"
    result = parse_where_to_filter(query)
    expected = models.Filter(
        must=[models.IsEmptyCondition(is_empty=models.PayloadField(key="attachments"))]
    )
    assert result == expected


def test_nested_array_projection():
    query = "diet[].food = 'meat' AND diet[].likes = true"
    result = parse_where_to_filter(query)
    expected = models.Filter(
        must=[
            models.FieldCondition(
                key="diet[].food", match=models.MatchValue(value="meat")
            ),
            models.FieldCondition(
                key="diet[].likes", match=models.MatchValue(value=True)
            ),
        ]
    )
    assert result == expected


@pytest.mark.parametrize(
    "query, expected_filter",
    [
        (
            "id = 123",
            models.Filter(must=[models.HasIdCondition(has_id=[123])]),
        ),
        (
            "id IN (1, 2, 3)",
            models.Filter(must=[models.HasIdCondition(has_id=[1, 2, 3])]),
        ),
        (
            f"id = '{uuid.UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479')}'", # hyphenated
            models.Filter(must=[models.HasIdCondition(has_id=[str(uuid.UUID('f47ac10b-58cc-4372-a567-0e02b2c3d479'))])]),
        ),
        (
            f"id = '{uuid.UUID('936DA01F9ABD4d9d80C702AF85C822A8')}'", # simple
            models.Filter(must=[models.HasIdCondition(has_id=[str(uuid.UUID('936DA01F9ABD4d9d80C702AF85C822A8'))])]),
        ),
        (
            f"id IN ('{uuid.UUID('550e8400-e29b-41d4-a716-446655440000')}', '{uuid.UUID('f9168c5e-ceb2-4faa-b6bf-329bf39fa1e4')}')", # hyphenated and URN
            models.Filter(must=[models.HasIdCondition(has_id=[str(uuid.UUID('550e8400-e29b-41d4-a716-446655440000')), str(uuid.UUID('f9168c5e-ceb2-4faa-b6bf-329bf39fa1e4'))])]),
        ),
    ],
)
def test_id_conditions(query, expected_filter):
    result = parse_where_to_filter(query)
    assert result == expected_filter


@pytest.mark.parametrize(
    "query, expected_filter",
    [
        (
            "tags IS EMPTY",
            models.Filter(
                must=[models.IsEmptyCondition(is_empty=models.PayloadField(key="tags"))]
            ),
        ),
        (
            "tags = []",
            models.Filter(
                must=[models.IsEmptyCondition(is_empty=models.PayloadField(key="tags"))]
            ),
        ),
    ],
)
def test_empty_conditions(query, expected_filter):
    result = parse_where_to_filter(query)
    assert result == expected_filter


@pytest.mark.parametrize(
    "query, expected_filter",
    [
        (
            "name LIKE 'Jo%hn%'",
            models.Filter(
                must=[models.FieldCondition(key="name", match=models.MatchText(text="Jo%hn%"))]
            ),
        ),
        (
            "name = 'John'",
            models.Filter(
                must=[models.FieldCondition(key="name", match=models.MatchValue(value="John"))]
            ),
        ),
    ],
)
def test_string_match_conditions(query, expected_filter):
    result = parse_where_to_filter(query)
    assert result == expected_filter


@pytest.mark.parametrize(
    "query, expected_filter",
    [
        (
            "price BETWEEN 10 AND 20",
            models.Filter(
                must=[models.FieldCondition(key="price", range=models.Range(gte=10, lte=20))]
            ),
        ),
        (
            "price NOT BETWEEN 10 AND 20",
            models.Filter(
                must_not=[
                    models.FieldCondition(key="price", range=models.Range(gte=10, lte=20))
                ]
            ),
        ),
        (
            "COUNT(tags) >= 3",
            models.Filter(
                must=[models.FieldCondition(key="tags", values_count=models.ValuesCount(gte=3))]
            ),
        ),
        (
            "COUNT(tags) BETWEEN 2 AND 5",
            models.Filter(
                must=[
                    models.FieldCondition(
                        key="tags", values_count=models.ValuesCount(gte=2, lte=5)
                    )
                ]
            ),
        ),
    ],
)
def test_range_and_count_conditions(query, expected_filter):
    result = parse_where_to_filter(query)
    assert result == expected_filter


@pytest.mark.parametrize(
    "query, expected_filter",
    [
        (
            "NOT (score < 50 OR attempts > 5)",
            models.Filter(
                must_not=[
                    models.Filter(
                        should=[
                            models.FieldCondition(key="score", range=models.Range(lt=50)),
                            models.FieldCondition(key="attempts", range=models.Range(gt=5)),
                        ]
                    )
                ]
            ),
        ),
    ],
)
def test_complex_negation_logic(query, expected_filter):
    result = parse_where_to_filter(query)
    assert result == expected_filter


@pytest.mark.parametrize(
    "query, expected_filter",
    [
        (
            "color NOT IN ('red', 'blue')",
            models.Filter(
                must_not=[
                    models.FieldCondition(
                        key="color", match=models.MatchAny(any=["red", "blue"])
                    )
                ]
            ),
        ),
        (
            "color NOT IN ()",
            models.Filter(
                must_not=[models.FieldCondition(key="color", match=models.MatchAny(any=[]))]
            ),
        ),
    ],
)
def test_not_in_conditions(query, expected_filter):
    result = parse_where_to_filter(query)
    assert result == expected_filter


@pytest.mark.parametrize(
    "query, expected_filter",
    [
        (
            "email IS NULL",
            models.Filter(
                must=[models.IsNullCondition(is_null=models.PayloadField(key="email"))]
            ),
        ),
        (
            "email IS NOT NULL",
            models.Filter(
                must_not=[models.IsNullCondition(is_null=models.PayloadField(key="email"))]
            ),
        ),
    ],
)
def test_is_null_and_not_null_conditions(query, expected_filter):
    result = parse_where_to_filter(query)
    assert result == expected_filter


def test_nested_and_or_not():
    query = "(a = 1 OR b = 2) AND NOT (c = 3 OR d = 4)"
    result = parse_where_to_filter(query)
    expected = models.Filter(
        must=[
            models.Filter(
                should=[
                    models.FieldCondition(key="a", match=models.MatchValue(value=1)),
                    models.FieldCondition(key="b", match=models.MatchValue(value=2)),
                ]
            )
        ],
        must_not=[
            models.Filter(
                should=[
                    models.FieldCondition(key="c", match=models.MatchValue(value=3)),
                    models.FieldCondition(key="d", match=models.MatchValue(value=4)),
                ]
            )
        ],
    )
    assert result == expected


# --- NESTED FIELD AND NESTED OBJECT FILTER TESTS ---
def test_dot_notation_nested_field():
    query = "country.name = 'Germany'"
    result = parse_where_to_filter(query)
    expected = models.Filter(
        must=[
            models.FieldCondition(
                key="country.name", match=models.MatchValue(value="Germany")
            )
        ]
    )
    assert result == expected


def test_array_projection_nested():
    query = "country.cities[].population >= 9.0"
    result = parse_where_to_filter(query)
    expected = models.Filter(
        must=[
            models.FieldCondition(
                key="country.cities[].population", range=models.Range(gte=9.0)
            )
        ]
    )
    assert result == expected