import asyncio
import aiohttp

from more_itertools import chunked

from models import Session, SwapiPerson, init_db, close_db

CHUNK_SIZE = 10
# список полей сведений о персонаже, по которым необходимо получить детальные данные
# (т.е. список названий дочерних записей через запятую)
DETAIL_FIELDS = [
    {'field': 'films', 'detail_field': 'title'},
    {'field': 'species', 'detail_field': 'name'},
    {'field': 'starships', 'detail_field': 'name'},
    {'field': 'vehicles', 'detail_field': 'name'},
]


# получить JSON-данные по заданному URL
async def get_json_by_url(url):
    session = aiohttp.ClientSession(trust_env=True)
    try:
        response = await session.get(url)
        json_data = await response.json()
    finally:
        await session.close()
    return json_data


# вставить данные о персонаже в БД
async def insert_people(people_list):
    people_list = [SwapiPerson(**person) for person in people_list]
    async with Session() as session:
        session.add_all(people_list)
        await session.commit()


# получить строку с названиями дочерних записей через запятую
# (названия фильмов, кораблей и т.п.)
async def get_details(url_list, field_name):
    # coros = [get_json_by_url(detail_url) for detail_url in url_list]
    # details_response_list = await asyncio.gather(*coros)
    # details = [detail.get(field_name) for detail in details_response_list]

    # вариант реализации синхронной загрузки детальных данных (названий фильмов, кораблей и т.д.)
    # (в рамкой корутины асинхронной загрузки данных по персонажу)
    details = []
    for details_url in url_list:
        json_data = await get_json_by_url(details_url)
        details.append(json_data.get(field_name))
    
    return ','.join(details)


# получить данные о персонаже
async def get_person(person_id):
    json_response = await get_json_by_url(f'https://swapi.dev/api/people/{person_id}/')
    if json_response is None:
        return None
    # удалить лишние поля (которые не требуется выгружать в БД)
    json_response.pop('created', None)
    json_response.pop('edited', None)
    json_response.pop('url', None)
    # получить названия дочерних записей
    for details in DETAIL_FIELDS:
        if details['field'] in json_response:
            json_response[details['field']] = await get_details(json_response[details['field']], details['detail_field'])
        # для предотвращения блокирования swapi-сервером по частоте обращений
        await asyncio.sleep(1)
    return json_response

async def main():
    await init_db()
    try:
        for person_id_chunk in chunked(range(1, 100), CHUNK_SIZE):
            coros = [get_person(person_id) for person_id in person_id_chunk]
            people_list = await asyncio.gather(*coros)
            asyncio.create_task(insert_people(people_list))
            # для предотвращения блокирования swapi-сервером по частоте обращений
            await asyncio.sleep(3)
        tasks = asyncio.all_tasks() - {asyncio.current_task()}
        await asyncio.gather(*tasks)
    finally:
        await close_db()

if __name__ == '__main__':
    asyncio.run(main())