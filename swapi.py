import asyncio
from aiohttp import ClientSession
from more_itertools import chunked
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, JSON

CHUNK_SIZE = 1


async def chunked_async(async_iter, size):
    buffer = []
    while True:
        try:
            item = await async_iter.__anext__()
        except StopAsyncIteration:
            break
        buffer.append(item)
        if len(buffer) == size:
            yield buffer
            buffer = []


PG_DSN = 'postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/sw'
engine = create_async_engine(PG_DSN)
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
Base = declarative_base()


class People(Base):
    __tablename__ = 'sw'

    id = Column(Integer, primary_key=True)
    json = Column(JSON)


async def get_person(person_id: int, session: ClientSession):
    async with session.get(f'https://swapi.dev/api/people/{person_id}') as response:
        json_data = await response.json()
        json_data['ID'] = person_id
        return json_data


async def get_item(url, d: dict, key1, key2, session):
    async with session.get(url) as response:
        json_data = await response.json()
        name = json_data[key1]
        d[key2] = name


async def get_items(urls: list, d: dict, key1, key2, session):
    l = []
    for url in urls:
        async with session.get(url) as response:
            json_data = await response.json()
            l.append(json_data[key1])
    l = ', '.join(l)
    d[key2] = l


async def gener():
    async with ClientSession() as session:
        for chunk in chunked(range(1, 120), CHUNK_SIZE):
            cor = [get_person(i, session) for i in chunk]
            results = await asyncio.gather(*cor)
            for result in results:
                if 'detail' not in result:
                    result2 = {key: val for key, val in result.items() if
                               key != 'created' and key != 'edited' and key != 'url'}

                    coro1 = get_item(result['homeworld'], result2, 'name', 'homeworld', session)
                    coro2 = get_items(result['films'], result2, 'title', 'films', session)
                    coro3 = get_items(result['species'], result2, 'name', 'species', session)
                    coro4 = get_items(result['starships'], result2, 'name', 'starships', session)
                    coro5 = get_items(result['vehicles'], result2, 'name', 'vehicles', session)
                    res = await asyncio.gather(coro1, coro2, coro3, coro4, coro5)
                    yield result2


async def insert_people(people_chunk):
    async with Session() as session:
        session.add_all([People(json=item) for item in people_chunk])
        await session.commit()


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

    async for chunk in chunked_async(gener(), CHUNK_SIZE):
        asyncio.create_task(insert_people(chunk))
    tasks = set(asyncio.all_tasks()) - {asyncio.current_task()}
    for task in tasks:
        await task


asyncio.run(main())
