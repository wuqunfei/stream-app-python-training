import asyncio
import aiomysql

loop = asyncio.get_event_loop()


async def test_example():
    conn = await aiomysql.connect(host='127.0.0.1', port=3306,
                                       user='root', password='feifeifei', db='leetcode',
                                       loop=loop)

    cur = await conn.cursor()
    await cur.execute("select * from Employee")
    print(cur.description)
    r = await cur.fetchall()
    for i in r:
        print(i)
    await cur.close()
    conn.close()

loop.run_until_complete(test_example())