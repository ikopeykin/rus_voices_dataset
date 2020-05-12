import asyncio
import pandas as pd

from asyncvkapi import AsyncVkApi as API
from database import DataBase as audiosDB
from settings import DSN, TOKEN, GROUP_ID


db = audiosDB(dsn=DSN)
api = API(token=TOKEN, group_id=GROUP_ID)

COLUMNS = ['sex', 'trash', 'ogg_audio', 'mp3_audio']
dataset_struct = {
    'sex': [],
    'trash': [],
    'ogg_audio': [],
    'mp3_audio': [],
}


splt = lambda A, n=100: [A[i:i+n] for i in range(0, len(A), n)]


async def get_audios(count=100000):
    return await db.get_audios(count=count)


async def get_users_sex(user_ids):
    # sex = 1, FEMALE
    # sex = 2, MALE

    N = 1000
    users = splt(user_ids, N)
    tasks = []
    to_ret = {}

    for user_pack in users:
        tasks.append(asyncio.create_task(
            api.users.get(user_ids=','.join(map(
                lambda s: str(s),
                user_pack
            )), fields='sex', count=N)
        ))

    resp = await asyncio.gather(*tasks)

    for r in resp:
        for user in r:
            to_ret[user['id']] = 'F' if user['sex'] == 1 else 'M'

    return to_ret


async def main():
    audios = await get_audios()
    user_ids = list(set(map(
        lambda u: u.get('user_id'),
        audios
    )))
    users_sex = await get_users_sex(user_ids)

    for audio in audios:
        dataset_struct['trash'].append(None)
        dataset_struct['sex'].append(users_sex[audio['user_id']])
        dataset_struct['mp3_audio'].append(f'{audio["filename"]}.mp3')
        dataset_struct['ogg_audio'].append(f'{audio["filename"]}.ogg')

    df = pd.DataFrame(dataset_struct, columns=COLUMNS)
    df.to_csv('dataset.csv', index=True, header=True)


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())

