import os
from dotenv import load_dotenv

from sqlalchemy import Integer, String
from sqlalchemy.ext.asyncio import (create_async_engine, async_sessionmaker,
                                    AsyncAttrs)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

load_dotenv()

POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "secret")
POSTGRES_USER = os.getenv("POSTGRES_USER", "app")
POSTGRES_DB = os.getenv("POSTGRES_DB", "app")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

PG_DSN = f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

engine = create_async_engine(PG_DSN)
Session = async_sessionmaker(engine, expire_on_commit=False)

class Base(DeclarativeBase, AsyncAttrs):
    pass

class SwapiPerson(Base):
    __tablename__ = 'swapi_person'

    # ID персонажа
    id: Mapped[int] = mapped_column(primary_key=True)
    birth_year: Mapped[str] = mapped_column(String(1000), nullable=True)
    eye_color: Mapped[str] = mapped_column(String(1000), nullable=True)
    # строка с названиями фильмов через запятую
    films: Mapped[str] = mapped_column(String(1000), nullable=True)
    gender: Mapped[str] = mapped_column(String(1000), nullable=True)
    hair_color: Mapped[str] = mapped_column(String(1000), nullable=True)
    height: Mapped[str] = mapped_column(String, nullable=True)
    homeworld: Mapped[str] = mapped_column(String(1000), nullable=True)
    mass: Mapped[str] = mapped_column(String, nullable=True)
    name: Mapped[str] = mapped_column(String(1000))
    skin_color: Mapped[str] = mapped_column(String(1000), nullable=True)
    # строка с названиями типов через запятую
    species: Mapped[str] = mapped_column(String(1000), nullable=True)
    # строка с названиями кораблей через запятую
    starships: Mapped[str] = mapped_column(String(1000), nullable=True)
    # строка с названиями транспорта через запятую
    vehicles: Mapped[str] = mapped_column(String(1000), nullable=True)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

async def close_db():
    await engine.dispose()
