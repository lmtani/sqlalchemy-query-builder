from pytest import fixture
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.types import JSON


@fixture(scope="session")
def Base():
    return declarative_base()


@fixture(scope="session")
def engine():
    return create_engine("mysql+pymysql://root:1234@127.0.0.1/teste", echo=False)


@fixture(scope="session")
def User(Base):
    class User(Base):
        __tablename__ = "users"

        id = Column(Integer, primary_key=True)
        name = Column(String(50))
        age = Column(Integer)
        meta = Column(JSON)

    return User


@fixture(scope="session")
def session(Base, engine, User):
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()
    user = User(
        name="Joao",
        age=3,
        meta={"food": {"loved": "rice", "cost": 10}},
    )
    user2 = User(
        name="Maria",
        age=6,
        meta={"food": {"loved": "brocolis", "cost": 30}},
    )
    user3 = User(
        name="Bento",
        age=30,
        meta={"food": {"loved": "rice", "cost": 10}},
    )
    session.add_all([user, user2, user3])
    session.commit()
    yield session
