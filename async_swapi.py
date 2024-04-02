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


# вставить данные о персонаже в БД
async def insert_people(people_list):
    swapi_people_list = []
    for person in people_list:
        try:
            swapi_person = SwapiPerson(**person)
            swapi_people_list.append(swapi_person)
        except Exception as err:
            print(f'Возникла следующая ошибка:\n{err}\nДанная ошибка возникла при обработке следующего персонажа:{person}')
            # pass
        
    async with Session() as session:        
        session.add_all(swapi_people_list)
        await session.commit()


# получить JSON-данные по заданному URL
async def get_json_by_url(url):
    async with aiohttp.ClientSession() as session:
        try:
            response = await session.get(url)
            json_data = await response.json()
            return json_data
        except Exception as err:
            print(f'Ошибка при обращении к swapi:\n{err}')


# получить строку с названиями дочерних записей через запятую
# (названия фильмов, кораблей и т.п.)
async def get_details(url_list, field_name):
    details = []
    for details_url in url_list:
        json_data = await get_json_by_url(details_url)
        if json_data is not None:
            details.append(json_data.get(field_name))

    # # варианта асинхронной реализации загрузки названий дочерних записей (названий фильмов, кораблей и т.д.)
    # coros = [get_json_by_url(detail_url) for detail_url in url_list]
    # details_response_list = await asyncio.gather(*coros)
    # details = [detail.get(field_name) for detail in details_response_list]
    
    return ','.join(details)


# получить данные о персонаже
async def get_person(person_id):
    # async with aiohttp.ClientSession() as session:
    print(f'get_person for id={person_id} - start')
    json_response = await get_json_by_url(
        f'https://swapi.dev/api/people/{person_id}/'
    )
    print(f'get_person for id={person_id} - json is given')
    if json_response is None or (json_response.get('detail', False) == 'Not found'):
    # if json_response is None:
        print(f'get_person for id={person_id} - data not found')
        return None
        
    # удалить лишние поля (которые не требуется выгружать в БД)
    json_response.pop('created', None)
    json_response.pop('edited', None)
    json_response.pop('url', None)
    
    print(f'get_person for id={person_id} - fields are popped')
    # получить названия дочерних записей
    for details in DETAIL_FIELDS:
        if details['field'] in json_response:
            json_response[details['field']] = await get_details(
                json_response[details['field']], 
                details['detail_field']
            )
        # для предотвращения блокирования swapi-сервером по частоте обращений
        # await asyncio.sleep(1)
    print(f'get_person for id={person_id} - details are obtained')
    
    return json_response

async def main():
    # # получить число персонажей в базе данных
    # json_reponse = await get_json_by_url('https://swapi.dev/api/people')
    # swapi_people_num = json_reponse.get('count', 100)    

    await init_db()
    try:
        for person_id_chunk in chunked(range(1, 100), CHUNK_SIZE):
            coros = [get_person(person_id) for person_id in person_id_chunk]
            people_list = await asyncio.gather(*coros)
            asyncio.create_task(insert_people(people_list))
            # для предотвращения блокирования swapi-сервером по частоте обращений
            # await asyncio.sleep(1)
        tasks = asyncio.all_tasks() - {asyncio.current_task()}
        await asyncio.gather(*tasks)
    finally:
        await close_db()

if __name__ == '__main__':
    asyncio.run(main())