from sqlalchemy.sql import select
from sqlalchemy.sql.expression import and_, or_


def build_query(model, filters):
    operators_map = {
        "eq": "__eq__",
        "ne": "__ne__",
        "lt": "__lt__",
        "le": "__le__",
        "gt": "__gt__",
        "ge": "__ge__",
        "in": "in_",
        "not_in": "notin_",
        "like": "like",
        "ilike": "ilike",
    }
    query = select(model)
    conditions = _prepare_conditions(filters, model, operators_map)
    if conditions:
        query = query.where(and_(*conditions))
    return query


def _prepare_conditions(filters, model, operators_map):
    conditions = []

    for f in filters:
        op = f["operator"]

        if op == "or":
            or_conditions = _prepare_conditions(f["value"], model, operators_map)
            conditions.append(or_(*or_conditions))

        elif op not in operators_map:
            raise ValueError(f"Unsupported operator {f['operator']}")

        elif "." in f["field"]:
            column = _extract_json_field(model, f)
            conditions.append(getattr(column, operators_map[op])(f["value"]))

        else:
            conditions.append(_create_condition(f, model, operators_map))
    return conditions


def _extract_json_field(model, filter_params):
    fields = filter_params["field"].split(".")
    column = getattr(model, fields.pop(0))
    for key in fields:
        column = column[key]
    return column


def _create_condition(filter_params, model, operators_map):
    return getattr(getattr(model, filter_params["field"]), operators_map[filter_params["operator"]])(
        filter_params["value"]
    )
