import asyncio
import aiohttp

from more_itertools import chunked

from models import Session, SwapiPerson, init_db, close_db

CHUNK_SIZE = 10
DETAIL_FIELDS = [
    {'field': 'films', 'detail_field': 'title'},
    {'field': 'species', 'detail_field': 'name'},
    {'field': 'starships', 'detail_field': 'name'},
    {'field': 'vehicles', 'detail_field': 'name'},
]

async def get_json_by_url(session, url):
    response = await session.get(url)
    json_data = await response.json()
    return json_data

async def insert_people(people_list):
    print(people_list)

async def get_details(session, url_list, field_name):
    details = []
    for details_url in url_list:
        json_data = await get_json_by_url(session, details_url)
        details.append(json_data.get(field_name))

    # coros = [get_json_by_url(session, detail_url) for detail_url in url_list]
    # details_response_list = await asyncio.gather(*coros)
    # details = [detail.get(field_name) for detail in details_response_list]
    
    return ','.join(details)



async def get_person(person_id):
    session = aiohttp.ClientSession()
    try:
        json_response = await get_json_by_url(session, f'https://swapi.dev/api/people/{person_id}/')
        for details in DETAIL_FIELDS:
            json_response[details['field']] = await get_details(session, json_response[details['field']], details['detail_field'])
        # json_response['species'] = await get_details(session, json_response['species'], 'name')
        # json_response['starships'] = await get_details(session, json_response['starships'], 'name')
        # json_response['vehicles'] = await get_details(session, json_response['vehicles'], 'name')
    finally:
        await session.close()
    return json_response

async def main():
    await init_db()
    try:
        for person_id_chunk in chunked(range(1, 10), CHUNK_SIZE):
            coros = [get_person(person_id) for person_id in person_id_chunk]
            people_list = await asyncio.gather(*coros)
            asyncio.create_task(insert_people(people_list))
        tasks = asyncio.all_tasks() - {asyncio.current_task()}
        await asyncio.gather(*tasks)
    finally:
        await close_db()

if __name__ == '__main__':
    asyncio.run(main())