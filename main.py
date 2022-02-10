from datetime import datetime
from dateutil.relativedelta import relativedelta
from dateutil import rrule
import asyncio
import aiohttp


async def get_latest_info():
    url = 'https://api.exchangerate.host/latest'
    async with aiohttp.ClientSession() as session:
        async with session.get(url, json={'base': 'USD'}) as response:
            data = await response.json()
            btc = data['rates']['BTC']
            print(btc)


async def historical_info(dates: list, session: aiohttp.client.ClientSession):
    for date in dates:
        url = f'https://api.exchangerate.host/{date}'
        async with session.get(url, json={'base': 'USD'}) as response:
            data = await response.json()
            btc = data['rates']['BTC']
            print(date, btc)


async def get_historical_info():
    dates = list()
    today = datetime.now()
    worker = 5
    tasks = list()
    for dt in rrule.rrule(rrule.DAILY, dtstart=today - relativedelta(years=1), until=today - relativedelta(days=1)):
        dates.append(str(datetime.date(dt)))
    async with aiohttp.ClientSession() as session:
        for i in range(worker):
            task = asyncio.create_task(historical_info(dates[i::worker], session))
            tasks.append(task)
        await asyncio.gather(*tasks)


def main():
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(get_latest_info())


if __name__ == '__main__':
    main()
