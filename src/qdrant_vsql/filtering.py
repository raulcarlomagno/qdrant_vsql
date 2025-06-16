from __future__ import annotations

from datetime import datetime
from typing import Any

from parsimonious.grammar import Grammar
from parsimonious.nodes import Node, NodeVisitor
from qdrant_client.http import models

# Define the grammar for the SQL-like WHERE clause using Parsimonious PEG format.
# This grammar defines the structure of valid queries.
qdrant_filter_grammar = Grammar(
    r"""
    # Entry rule for the entire expression
    expression = factor (ws (OR / AND) ws factor)*

    # A factor can be a NOT of any grouped or atomic condition
    factor = (NOT ws)? (condition / ("(" ws expression ws ")"))

    # A condition can be one of several types. Order is important for parsing.
    condition = is_null_condition / is_empty_condition / is_empty_array_condition / values_count_condition / has_id_condition / comparison

    # Comparison operators and their operands, now including BETWEEN and NOT IN/NOT BETWEEN
    comparison = identifier ws comparison_op
    comparison_op = (not_in_op ws value) / (not_between_op ws value ws AND ws value) / (gte ws value) / (lte ws value) / (gt ws value) / (lt ws value) / (equals ws value) / (not_equals ws value) / (in_op ws value) / (like_op ws value) / (between_op ws value ws AND ws value)

    # Specific condition types with more precise matching
    is_null_condition = identifier ws IS ws NOT? ws NULL
    is_empty_condition = identifier ws IS ws EMPTY
    is_empty_array_condition = identifier ws equals ws "[]"
    has_id_condition = "id" ws (equals / not_equals / in_op / not_in_op) ws value
    values_count_condition = COUNT "(" ws identifier ws ")" ws values_count_op
    values_count_op = (gte ws number) / (lte ws number) / (gt ws number) / (lt ws number) / (equals ws number) / (between_op ws number ws AND ws number)

    # Operators
    equals = "="
    not_equals = "!=" / "<>"
    gte = ">="
    lte = "<="
    gt = ">"
    lt = "<"
    in_op = IN
    not_in_op = NOT ws IN
    like_op = LIKE
    between_op = BETWEEN
    not_between_op = NOT ws BETWEEN

    # Value types
    value = list_value / "[]" / string / number / boolean / date_string

    # Allow empty list for IN/NOT IN
    list_value = "(" ws (value (ws "," ws value)*)? ws ")"
    string = ~r"'(?:[^'\\]|\\['\\])*'"
    number = ~r"-?\d+(?:\.\d+)?"
    boolean = TRUE / FALSE
    date_string = ~r"'\\d{4}-\\d{2}-\\d{2}(T\\d{2}:\\d{2}:\\d{2}(\\.\\d+)?Z?)?'"

    # Keywords (case-insensitive, must not be followed by a word char)
    AND = ~r"and(?![a-zA-Z0-9_])"i
    OR = ~r"or(?![a-zA-Z0-9_])"i
    NOT = ~r"not(?![a-zA-Z0-9_])"i
    IN = ~r"in(?![a-zA-Z0-9_])"i
    LIKE = ~r"like(?![a-zA-Z0-9_])"i
    IS = ~r"is(?![a-zA-Z0-9_])"i
    NULL = ~r"null(?![a-zA-Z0-9_])"i
    EMPTY = ~r"empty(?![a-zA-Z0-9_])"i
    TRUE = ~r"true(?![a-zA-Z0-9_])"i
    FALSE = ~r"false(?![a-zA-Z0-9_])"i
    BETWEEN = ~r"between(?![a-zA-Z0-9_])"i
    COUNT = ~r"count(?![a-zA-Z0-9_])"i

    # Identifier for field names
    identifier = ~r"[a-zA-Z_][a-zA-Z0-9_\[\].]*"

    # Whitespace (to be ignored by the visitor)
    ws = ~r"\s*"
    """,  # noqa: E501
)


QDRANT_FILTER_TYPES = (
    models.FieldCondition,
    models.IsEmptyCondition,
    models.IsNullCondition,
    models.HasIdCondition,
    models.HasVectorCondition,
    models.NestedCondition,
    models.Filter,
)


class QdrantFilterVisitor(NodeVisitor):
    """Traverse the parse tree and build a Qdrant Filter object.

    Each method corresponds to a rule in the grammar.
    """

    def visit_expression(self, node: Node, visited_children: Any) -> models.Filter:
        result: Any = visited_children[0]
        rest: Any = visited_children[1]

        def ensure_list(val: Any) -> list[Any]:
            if val is None:
                return []
            if isinstance(val, list):
                return val
            return [val]

        def dedup(lst: list[Any]) -> list[Any]:
            # Remove duplicates while preserving order
            seen: set = set()
            out: list[Any] = []
            for x in lst:
                if id(x) not in seen:
                    out.append(x)
                    seen.add(id(x))
            return out

        def merge_filters(op: str, left: Any, right: Any) -> models.Filter:
            if op == "AND":
                must_conditions: list[Any] = []
                must_not_conditions: list[Any] = []

                def collect_conditions(
                    f: Any,
                    target_must: list[Any],
                    target_must_not: list[Any],
                ) -> None:
                    if isinstance(f, models.Filter):
                        if f.must:
                            target_must.extend(ensure_list(f.must))
                        if f.must_not:
                            target_must_not.extend(ensure_list(f.must_not))
                        if f.should and not f.must and not f.must_not:
                            # A pure OR filter becomes a 'must' condition in an AND context. # noqa: E501
                            target_must.append(f)
                        elif f.should and (f.must or f.must_not):
                            # If it's a complex filter with should and other parts, # noqa: E501
                            # add the whole filter.
                            target_must.append(f)
                    elif f is not None:
                        target_must.append(f)

                collect_conditions(left, must_conditions, must_not_conditions)
                collect_conditions(right, must_conditions, must_not_conditions)

                must_conditions = dedup(_clean_filter_list(must_conditions))
                must_not_conditions = dedup(_clean_filter_list(must_not_conditions))
                return models.Filter(
                    must=must_conditions or None,
                    must_not=must_not_conditions or None,
                )

            # OR
            def flatten_should(f: Any) -> list[Any]:
                f = _unwrap_group(f)
                out: list[Any] = []
                if isinstance(f, models.Filter):
                    # If it's an OR filter already, flatten its shoulds
                    if f.should and not f.must and not f.must_not:
                        out.extend(ensure_list(f.should))
                    # Otherwise, add the whole filter
                    else:
                        out.append(f)
                elif f is not None:
                    out.append(f)
                return out

            should: list[Any] = flatten_should(left) + flatten_should(right)
            should = dedup(_clean_filter_list(should))
            return models.Filter(should=should or None)

        for op, term in rest:
            op_str: str | None = None
            if hasattr(op, "text"):
                op_str = op.text.upper()
            elif isinstance(op, list):
                for o in op:
                    if hasattr(o, "text"):
                        op_str = o.text.upper()
                        break
                    if isinstance(o, str):
                        op_str = o.upper()
                        break
            if not op_str and isinstance(op, str):
                op_str = op.upper()
            if not op_str:
                msg = f"Could not extract logical operator from {op}"
                raise ValueError(msg)
            result = merge_filters(op_str, result, term)
        if not isinstance(result, models.Filter):
            return models.Filter(must=_clean_filter_list([result]))
        if result.must:
            result.must = _clean_filter_list(result.must)
            if not result.must:
                result.must = None
        if result.should:
            result.should = _clean_filter_list(result.should)
            if not result.should:
                result.should = None
        if result.must_not:
            result.must_not = _clean_filter_list(result.must_not)
            if not result.must_not:
                result.must_not = None
        return result

    def visit_factor(self, node: Node, visited_children: Any) -> models.Filter:
        """Handle NOT for any logic_term (including parenthesized expressions)."""
        not_part: Any = visited_children[0]
        term: Any = _unwrap_group(visited_children[1])

        if not_part:  # Only apply NOT logic if not_part is not empty
            if isinstance(term, models.Filter):
                # Handle double negation: NOT (must_not [...]) -> must [...]
                if term.must_not and not term.must and not term.should:
                    return models.Filter(must=term.must_not)

                # If the term is a Filter, we want to negate the *entire* filter.
                # So, we put the original filter inside a must_not list.
                return models.Filter(must_not=[term])
            # If it's a simple condition (not a Filter object), just wrap it in must_not
            return models.Filter(must_not=[term] if term is not None else None)

        # If NOT is not present, just return the term as-is
        return term

    def visit_condition(self, node: Node, visited_children: Any) -> Any:
        return visited_children[0]

    def visit_comparison(self, node: Node, visited_children: Any) -> models.Filter:
        """Handle all standard comparisons: =, !=, >, IN, BETWEEN, NOT IN, NOT BETWEEN."""  # noqa: E501
        identifier: str = visited_children[0]
        op_details: Any = (
            visited_children[2][0]
            if isinstance(visited_children[2], list) and visited_children[2]
            else visited_children[2]
        )
        if not isinstance(op_details, list):
            op_details = [op_details]
        op_text: Any = op_details[0]
        # Normalize op_text to a string for all cases and convert to uppercase
        # for handler lookup.
        if isinstance(op_text, list):
            op_text = " ".join(str(x) for x in op_text).upper()
        else:
            op_text = str(op_text).upper()
        value: Any = op_details[1] if len(op_details) > 1 else None

        def create_field_condition(key: str, **kwargs: Any) -> models.FieldCondition:
            return models.FieldCondition(key=key, **kwargs)

        def handle_between(
            val1: Any, val2: Any, *, is_not: bool = False
        ) -> models.Filter:
            range_kwargs = {"gte": val1, "lte": val2}
            if isinstance(val1, str) and isinstance(val2, str):
                try:
                    datetime.fromisoformat(val1.replace("Z", "+00:00"))
                    datetime.fromisoformat(val2.replace("Z", "+00:00"))
                    cond = create_field_condition(
                        identifier,
                        range=models.DatetimeRange(**range_kwargs),
                    )
                    return (
                        models.Filter(must_not=[cond])
                        if is_not
                        else models.Filter(must=[cond])
                    )
                except (ValueError, TypeError):
                    pass
            cond = create_field_condition(
                identifier,
                range=models.Range(**range_kwargs),
            )
            return (
                models.Filter(must_not=[cond]) if is_not else models.Filter(must=[cond])
            )

        def handle_equality(val: Any, *, is_not: bool = False) -> models.Filter:
            if val is None:
                msg = f"Missing value for comparison on field {identifier}"
                raise ValueError(msg)
            cond = create_field_condition(
                identifier,
                match=models.MatchValue(value=val),
            )
            return (
                models.Filter(must_not=[cond]) if is_not else models.Filter(must=[cond])
            )

        def handle_in(val: Any, *, is_not: bool = False) -> models.Filter:
            if val is None:
                msg = f"Missing value for 'IN' comparison on field {identifier}"
                raise ValueError(msg)
            clean_value = self._flatten_all(val)
            if is_not:
                cond = create_field_condition(
                    identifier,
                    match=models.MatchExcept(**{"except": clean_value}),
                )
                return models.Filter(must=[cond])
            cond = create_field_condition(
                identifier,
                match=models.MatchAny(any=clean_value),
            )
            return models.Filter(must=[cond])

        def handle_like(val: Any) -> models.Filter:
            if val is None:
                msg = f"Missing value for 'LIKE' comparison on field {identifier}"
                raise ValueError(msg)
            cond = create_field_condition(identifier, match=models.MatchText(text=val))
            return models.Filter(must=[cond])

        def handle_range(op: str, val: Any) -> models.Filter:
            range_kwargs = {}
            if op == ">":
                range_kwargs["gt"] = val
            elif op == ">=":
                range_kwargs["gte"] = val
            elif op == "<":
                range_kwargs["lt"] = val
            elif op == "<=":
                range_kwargs["lte"] = val

            if isinstance(val, str):
                try:
                    datetime.fromisoformat(val.replace("Z", "+00:00"))
                    cond = create_field_condition(
                        identifier,
                        range=models.DatetimeRange(**range_kwargs),
                    )
                    return models.Filter(must=[cond])
                except (ValueError, TypeError):
                    pass
            cond = create_field_condition(
                identifier,
                range=models.Range(**range_kwargs),
            )
            return models.Filter(must=[cond])

        # Dispatch table for operators
        op_handlers = {
            "BETWEEN": lambda: handle_between(op_details[1], op_details[3]),
            "NOT BETWEEN": lambda: handle_between(
                op_details[1],
                op_details[3],
                is_not=True,
            ),
            "=": lambda: handle_equality(value),
            "!=": lambda: handle_equality(value, is_not=True),
            "<>": lambda: handle_equality(value, is_not=True),
            "IN": lambda: handle_in(value),
            "NOT IN": lambda: handle_in(value, is_not=True),
            "LIKE": lambda: handle_like(value),
            ">": lambda: handle_range(op_text, value),
            ">=": lambda: handle_range(op_text, value),
            "<": lambda: handle_range(op_text, value),
            "<=": lambda: handle_range(op_text, value),
        }

        handler = op_handlers.get(op_text)
        if handler:
            return handler()
        msg = f"Unsupported comparison operator: {op_text}"
        raise ValueError(msg)

    def visit_is_null_condition(
        self,
        _node: Node,
        visited_children: Any,
    ) -> models.Filter:
        """Handle IS NULL and IS NOT NULL conditions."""
        identifier = visited_children[0]
        # The structure is typically: [identifier, ws, IS, ws, NOT?, ws, NULL]
        # We need to check if 'NOT' is present.
        # The 'NOT' node is usually at index 4 if present, otherwise it's None or empty.
        # The 'NOT?' part is at index 4. It will be a list: [] if not present,
        # or [Node('NOT'), Node('ws')] if present.
        not_op_present = bool(visited_children[4])
        condition = models.IsNullCondition(is_null=models.PayloadField(key=identifier))
        if not_op_present:
            return models.Filter(must_not=[condition])
        return models.Filter(must=[condition])

    def visit_is_empty_condition(
        self,
        _node: Node,
        visited_children: Any,
    ) -> models.Filter:
        """Handle IS EMPTY condition."""
        cond = models.IsEmptyCondition(
            is_empty=models.PayloadField(key=visited_children[0]),
        )
        return models.Filter(must=[cond])

    def visit_is_empty_array_condition(
        self,
        _node: Node,
        visited_children: Any,
    ) -> models.Filter:
        """Handle = [] (empty array) condition."""
        # This condition is semantically equivalent to IS EMPTY for Qdrant
        # Delegate to visit_is_empty_condition
        return self.visit_is_empty_condition(_node, visited_children)

    def visit_has_id_condition(
        self,
        _node: Node,
        visited_children: Any,
    ) -> models.Filter:
        """Handle id = ..., id IN (...), id != ..., id <>, and id NOT IN (...) conditions."""  # noqa: E501
        meaningful_parts = [
            child for child in visited_children if child is not None and child != "id"
        ]

        if len(meaningful_parts) >= 2:  # noqa: PLR2004
            op_node = meaningful_parts[0]
            value_node = meaningful_parts[1]

            op_raw = op_node  # This will be ['='] or [['<>']] or [['NOT', 'IN']]

            # Flatten the list to get the actual operator string(s)
            if isinstance(op_raw, list):
                # If it's [['<>']] or [['NOT', 'IN']]
                if len(op_raw) == 1 and isinstance(op_raw[0], list):
                    op_str = " ".join(str(x) for x in op_raw[0]).upper()
                # If it's ['='] or ['IN']
                elif len(op_raw) == 1 and isinstance(op_raw[0], str):
                    op_str = op_raw[0].upper()
                else:
                    msg = f"Unexpected operator format: {op_raw}"
                    raise ValueError(msg)
            elif isinstance(
                op_raw,
                str,
            ):  # Should not happen based on trace, but good for robustness
                op_str = op_raw.upper()
            else:
                msg = f"Could not extract operator string from {op_raw}"
                raise ValueError(msg)

            is_not_condition = False
            normalized_op_str = op_str

            if op_str in ["!=", "<>"]:
                is_not_condition = True
                normalized_op_str = "="
            elif op_str == "NOT IN":
                is_not_condition = True
                normalized_op_str = "IN"

            if normalized_op_str == "IN":
                has_id_values = self._flatten_all(value_node)
                cond = models.HasIdCondition(has_id=has_id_values)
            elif normalized_op_str == "=":
                has_id_values = (
                    [value_node] if not isinstance(value_node, list) else value_node
                )
                cond = models.HasIdCondition(has_id=has_id_values)
            else:
                msg = f"Unexpected normalized operator for id condition: {normalized_op_str}"  # noqa: E501
                raise ValueError(msg)

            if is_not_condition:
                return models.Filter(must_not=[cond])
            return models.Filter(must=[cond])
        msg = f"Unexpected children for has_id_condition: {visited_children}"
        raise ValueError(msg)

    def visit_values_count_condition(
        self,
        _node: Node,
        visited_children: Any,
    ) -> models.Filter:
        """Handle COUNT(field) conditions."""
        identifier = visited_children[3]
        op_details = (
            visited_children[7][0]
            if isinstance(visited_children[7], list) and visited_children[7]
            else visited_children[7]
        )
        if not isinstance(op_details, list):
            op_details = [op_details]
        op_text = str(op_details[0]).upper()  # Convert to uppercase for handler lookup
        count_kwargs = {}
        if op_text == "BETWEEN":
            val1 = op_details[1]
            val2 = op_details[3]
            count_kwargs["gte"] = val1
            count_kwargs["lte"] = val2
        else:
            number = op_details[1]
            if op_text == ">":
                count_kwargs["gt"] = number
            elif op_text == ">=":
                count_kwargs["gte"] = number
            elif op_text == "<":
                count_kwargs["lt"] = number
            elif op_text == "<=":
                count_kwargs["lte"] = number
            elif op_text == "=":
                count_kwargs["gte"] = count_kwargs["lte"] = number
        cond = models.FieldCondition(
            key=identifier,
            values_count=models.ValuesCount(**count_kwargs),
        )
        return models.Filter(must=[cond])

    @staticmethod
    def _flatten_all(val: Any) -> list[Any]:
        if isinstance(val, list):
            out: list[Any] = []
            for v in val:
                out.extend(QdrantFilterVisitor._flatten_all(v))
            return out
        if val is not None:
            return [val]
        return []

    @staticmethod
    def _flatten_list_values(x: Any) -> list[Any]:
        out = []
        if isinstance(x, list):
            for i in x:
                out.extend(QdrantFilterVisitor._flatten_list_values(i))
        elif x is not None and x not in {"", ","}:
            out.append(x)
        return out

    def visit_value(self, _node: Node, visited_children: Any) -> Any:
        """Extract value for string, number, boolean, date_string, list_value, or empty list."""  # noqa: E501
        val = visited_children[0]

        if isinstance(val, list):
            return self._flatten_all(val)
        if isinstance(val, str):
            if val.startswith("'") and val.endswith("'"):
                return val[1:-1].replace("\\'", "'").replace("\\\\", "\\")
            return val
        return val

    def visit_list_value(self, _node: Node, visited_children: Any) -> list[Any]:
        """Extract list of values from a parenthesized value list."""
        values = []
        # The content inside the parentheses is at index 2 if it exists
        # It can be empty, or contain a value and a list of (ws, ',', ws, value) tuples
        if len(visited_children) > 2 and visited_children[2]:  # noqa: PLR2004
            content = visited_children[2]
            # content[0] is the first value
            values.extend(self._flatten_list_values(content[0]))
            # content[1] is the list of (ws, ',', ws, value) tuples
            if len(content) > 1 and isinstance(content[1], list):
                for tail_item in content[1]:
                    if len(tail_item) > 3:  # noqa: PLR2004
                        values.extend(self._flatten_list_values(tail_item[3]))
        return values

    def visit_string(self, _node: Node, _visited_children: Any) -> str:
        """Extract string value, unquoting and unescaping as needed."""
        return _node.text[1:-1].replace("\\'", "'").replace("\\\\", "\\")

    def visit_number(self, _node: Node, _visited_children: Any) -> int | float:
        """Extract number value as int or float."""
        return float(_node.text) if "." in _node.text else int(_node.text)

    def visit_boolean(self, _node: Node, _visited_children: Any) -> bool:
        """Extract boolean value."""
        return _node.text.lower() == "true"

    def visit_date_string(self, _node: Node, _visited_children: Any) -> str:
        """Extract date string value, unquoting as needed."""
        return _node.text[1:-1]

    def visit_identifier(self, _node: Node, _visited_children: Any) -> str:
        """Extract identifier string."""
        return _node.text

    def generic_visit(self, _node: Node, visited_children: Any) -> Any:
        """Remove all None values from visited children."""
        return [child for child in visited_children if child is not None] or _node.text

    def visit_ws(self, _node: Node, _visited_children: Any) -> None:
        """Discard whitespace nodes."""
        return


def _clean_filter_list(lst: Any) -> list[Any]:
    """Remove any non-Qdrant objects (like '(', ')', None, whitespace) from lists."""
    out = []
    for x in lst:
        if x is None:
            continue
        if isinstance(x, QDRANT_FILTER_TYPES):
            out.append(x)
        elif isinstance(x, list):
            out.extend(_clean_filter_list(x))
    return out


def _unwrap_group(val: Any) -> Any:  # noqa: PLR0911
    """Recursively extract the first valid Qdrant object from lists/tuples."""
    if isinstance(val, models.Filter):
        must = (
            val.must
            if isinstance(val.must, list)
            else ([val.must] if val.must is not None else [])
        )
        should = (
            val.should
            if isinstance(val.should, list)
            else ([val.should] if val.should is not None else [])
        )
        must_not = (
            val.must_not
            if isinstance(val.must_not, list)
            else ([val.must_not] if val.must_not is not None else [])
        )
        if must and len(must) == 1 and not should and not must_not:
            return _unwrap_group(must[0])
        if should and len(should) == 1 and not must and not must_not:
            return _unwrap_group(should[0])
        return val
    if isinstance(val, QDRANT_FILTER_TYPES):
        return val
    if isinstance(val, (list, tuple)):
        for v in val:
            unwrapped = _unwrap_group(v)
            if unwrapped is not None:
                return unwrapped
        return None
    return None


def where2filter(where_clause: str) -> models.Filter:
    """Parse a SQL-like WHERE clause string into a Qdrant Filter object."""
    tree = qdrant_filter_grammar.parse(where_clause)
    visitor = QdrantFilterVisitor()
    result = visitor.visit(tree)
    if not isinstance(result, models.Filter):
        if result:
            return models.Filter(must=[result])
        return models.Filter()
    return result
