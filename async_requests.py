import asyncio
import datetime
import aiohttp
import more_itertools
from models import Session, SwapiPeople, close_orm, init_orm

MAX_COROS = 10

async def get_people(person_id, http_session):
    url = f"https://swapi.py4e.com/api/people/{person_id}/"
    http_response = await http_session.get(url)
    data = await http_response.json()
    if http_response.status == 200:
        data['id'] = person_id
        homeworld_response = await http_session.get(data.get('homeworld'))
        homeworld_data = await homeworld_response.json()
        data['homeworld'] = homeworld_data.get('name')
        films_list, species_list, starships_list, vehicles_list = list() , list(), list(), list()
        for film in data.get('films'):
            film_response = await http_session.get(film)
            film_data = await film_response.json()
            films_list.append(film_data.get('title'))
        for specie in data.get('species'):
            specie_response = await http_session.get(specie)
            specie_data = await specie_response.json()
            species_list.append(specie_data.get('name'))
        for ship in data.get('starships'):
            ship_response = await http_session.get(ship)
            ship_data = await ship_response.json()
            starships_list.append(ship_data.get('name'))
        for vehicle in data.get('vehicles'):
            vehicle_response = await http_session.get(vehicle)
            vehicle_data = await vehicle_response.json()
            vehicles_list.append(vehicle_data.get('name'))
        data['films'] = ', '.join(films_list)
        data['species'] = ', '.join(species_list)
        data['starships'] = ', '.join(starships_list)
        data['vehicles'] = ', '.join(vehicles_list)
        del data['url'], data['created'], data['edited']
    return data

async def insert_people(json_list):
    async with Session() as session:
        swapi_people_list = [SwapiPeople(**hero) for hero in json_list if 'detail' not in hero]
        session.add_all(swapi_people_list)
        await session.commit()

async def main():
    await init_orm()

    async with aiohttp.ClientSession() as http_session:
        for i_list in more_itertools.chunked(range(1, 101), MAX_COROS):
            coros = [get_people(i, http_session) for i in i_list]
            result = await asyncio.gather(*coros)
            coro = insert_people(result)
            asyncio.create_task(coro)

        tasks = asyncio.all_tasks()
        task_main = asyncio.current_task()
        tasks.remove(task_main)
        await asyncio.gather(*tasks)

    await close_orm()

start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
