from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select
from sqlalchemy.sql.expression import and_, or_
from sqlalchemy.types import JSON


engine = create_engine("mysql+pymysql://root:1234@127.0.0.1/teste", echo=True)
Base = declarative_base()


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    name = Column(String(50))
    email = Column(String(50))
    age = Column(Integer)
    adicional = Column(JSON)


Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)


Session = sessionmaker(bind=engine)
session = Session()
user = User(name="Joao", email="a@gmail.com", age=3, adicional={"comida": {"favorita": "arroz", "custo": 10}})
user2 = User(name="Maria", email="b@gmail.com", age=6, adicional={"comida": {"favorita": "brocolis", "custo": 30}})
user3 = User(name="Bento", email="c@gmail.com", age=30, adicional={"comida": {"favorita": "arroz", "custo": 10}})
session.add_all([user, user2, user3])
session.commit()


def build_query(model, filters_):
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
    conditions = _prepare_conditions(filters_, model, operators_map)
    if conditions:
        query = query.where(and_(*conditions))
    return query


def _prepare_conditions(filters_, model, operators_map, conditions=None):
    if not conditions:
        conditions = []

    for f in filters_:
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


filters = [
    {"field": "adicional.comida.favorita", "operator": "eq", "value": "arroz"},
    {"field": "age", "operator": "gt", "value": 10},
]

test_query = build_query(User, filters)
result = list(session.execute(test_query).scalars())
assert len(result) == 1
assert result[0].name == "Bento"


filters = [
    {"field": "adicional.comida.favorita", "operator": "eq", "value": "brocolis"},
    {"field": "age", "operator": "lt", "value": 10},
]

test_query = build_query(User, filters)
result = list(session.execute(test_query).scalars())
assert len(result) == 1
assert result[0].name == "Maria"


filters = [
    {"field": "adicional.comida.favorita", "operator": "eq", "value": "arroz"},
    {"field": "age", "operator": "lt", "value": 100},
]

test_query = build_query(User, filters)
result = list(session.execute(test_query).scalars())
assert len(result) == 2
assert result[0].name == "Joao"
assert result[1].name == "Bento"


filters = [
    {"field": "adicional.comida.custo", "operator": "lt", "value": 11},
]

test_query = build_query(User, filters)
result = list(session.execute(test_query).scalars())
assert len(result) == 2
assert result[0].name == "Joao"
assert result[1].name == "Bento"


filters = [
    {
        "operator": "or",
        "value": [{"field": "age", "operator": "eq", "value": 3}, {"field": "age", "operator": "eq", "value": 6}],
    },
]

test_query = build_query(User, filters)
result = list(session.execute(test_query).scalars())
assert len(result) == 2
assert result[0].name == "Joao"


filters = [
    {
        "operator": "or",
        "value": [
            {"field": "name", "operator": "eq", "value": "Maria"},
            {"field": "age", "operator": "lt", "value": 4},
        ],
    },
]

test_query = build_query(User, filters)
result = list(session.execute(test_query).scalars())
assert len(result) == 2
assert result[0].name == "Joao"
assert result[1].name == "Maria"


filters = [
    {
        "operator": "or",
        "value": [
            {"field": "name", "operator": "eq", "value": "Maria"},
            {"field": "adicional.comida.favorita", "operator": "eq", "value": "arroz"},
        ],
    },
]

test_query = build_query(User, filters)
result = list(session.execute(test_query).scalars())
assert len(result) == 3
assert result[0].name == "Joao"
assert result[1].name == "Maria"
assert result[2].name == "Bento"


filters = [
    {
        "operator": "or",
        "value": [
            {"field": "name", "operator": "eq", "value": "Maria"},
            {"field": "adicional.comida.favorita", "operator": "eq", "value": "arroz"},
        ],
    },
    {"field": "age", "operator": "lt", "value": 20},
]

test_query = build_query(User, filters)
result = list(session.execute(test_query).scalars())
assert len(result) == 2
assert result[0].name == "Joao"
assert result[1].name == "Maria"


# Pq do \"? Veja:
# https://stackoverflow.com/a/57491053
filters = [
    {
        "operator": "or",
        "value": [
            {"field": "name", "operator": "eq", "value": "Maria"},
            {"field": "adicional.comida.favorita", "operator": "like", "value": '"arr%'},
        ],
    },
]

test_query = build_query(User, filters)
result = list(session.execute(test_query).scalars())

assert len(result) == 3
assert result[0].name == "Joao"
assert result[1].name == "Maria"
assert result[2].name == "Bento"


filters = [
    {"field": "adicional.comida.favorita", "operator": "not_in", "value": ["brocolis", "couve"]},
]

test_query = build_query(User, filters)
result = list(session.execute(test_query).scalars())
for i in result:
    print(i.name)
assert len(result) == 2
assert result[0].name == "Joao"
assert result[1].name == "Bento"

print("✓")
# SELECT users.id, users.name, users.email, users.age, users.adicional
# FROM users
# WHERE users.name = "Maria" OR JSON_EXTRACT(JSON_EXTRACT(users.adicional, '$."comida"'), '$."favorita"') LIKE 'arr%';
