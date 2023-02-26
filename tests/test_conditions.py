from src import build_query


def test_filters_1(session, User):
    filters = [
        {"field": "meta.food.loved", "operator": "eq", "value": "rice"},
        {"field": "age", "operator": "gt", "value": 10},
    ]

    test_query = build_query(User, filters)
    result = list(session.execute(test_query).scalars())
    assert len(result) == 1
    assert result[0].name == "Bento"


def test_filters_2(session, User):
    filters = [
        {"field": "meta.food.loved", "operator": "eq", "value": "brocolis"},
        {"field": "age", "operator": "lt", "value": 10},
    ]

    test_query = build_query(User, filters)
    result = list(session.execute(test_query).scalars())
    assert len(result) == 1
    assert result[0].name == "Maria"


def test_filters_3(session, User):
    filters = [
        {"field": "meta.food.loved", "operator": "eq", "value": "rice"},
        {"field": "age", "operator": "lt", "value": 100},
    ]

    test_query = build_query(User, filters)
    result = list(session.execute(test_query).scalars())
    assert len(result) == 2
    assert result[0].name == "Joao"
    assert result[1].name == "Bento"


def test_filters_4(session, User):
    filters = [
        {"field": "meta.food.cost", "operator": "lt", "value": 11},
    ]

    test_query = build_query(User, filters)
    result = list(session.execute(test_query).scalars())
    assert len(result) == 2
    assert result[0].name == "Joao"
    assert result[1].name == "Bento"


def test_filters_5(session, User):
    filters = [
        {
            "operator": "or",
            "value": [
                {"field": "age", "operator": "eq", "value": 3},
                {"field": "age", "operator": "eq", "value": 6},
            ],
        },
    ]

    test_query = build_query(User, filters)
    result = list(session.execute(test_query).scalars())
    assert len(result) == 2
    assert result[0].name == "Joao"


def test_filters_6(session, User):
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


def test_filters_7(session, User):
    filters = [
        {
            "operator": "or",
            "value": [
                {"field": "name", "operator": "eq", "value": "Maria"},
                {
                    "field": "meta.food.loved",
                    "operator": "eq",
                    "value": "rice",
                },
            ],
        },
    ]

    test_query = build_query(User, filters)
    result = list(session.execute(test_query).scalars())
    assert len(result) == 3
    assert result[0].name == "Joao"
    assert result[1].name == "Maria"
    assert result[2].name == "Bento"


def test_filters_8(session, User):
    filters = [
        {
            "operator": "or",
            "value": [
                {"field": "name", "operator": "eq", "value": "Maria"},
                {
                    "field": "meta.food.loved",
                    "operator": "eq",
                    "value": "rice",
                },
            ],
        },
        {"field": "age", "operator": "lt", "value": 20},
    ]

    test_query = build_query(User, filters)
    result = list(session.execute(test_query).scalars())
    assert len(result) == 2
    assert result[0].name == "Joao"
    assert result[1].name == "Maria"


def test_filters_9(session, User):
    # Pq do \"? Veja:
    # https://stackoverflow.com/a/57491053
    filters = [
        {
            "operator": "or",
            "value": [
                {"field": "name", "operator": "eq", "value": "Maria"},
                {
                    "field": "meta.food.loved",
                    "operator": "like",
                    "value": '"ric%',
                },
            ],
        },
    ]

    test_query = build_query(User, filters)
    result = list(session.execute(test_query).scalars())

    assert len(result) == 3
    assert result[0].name == "Joao"
    assert result[1].name == "Maria"
    assert result[2].name == "Bento"


def test_filters_10(session, User):
    filters = [
        {
            "field": "meta.food.loved",
            "operator": "not_in",
            "value": ["brocolis", "couve"],
        },
    ]

    test_query = build_query(User, filters)
    result = list(session.execute(test_query).scalars())
    for i in result:
        print(i.name)
    assert len(result) == 2
    assert result[0].name == "Joao"
    assert result[1].name == "Bento"
