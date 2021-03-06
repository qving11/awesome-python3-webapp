import orm
import asyncio
from models import User, Blog, Comment

async def test(loop):
    await orm.create_pool(loop=loop, user='root', password='password', db='awesome')
    u = User(name='Mike', email='mike@163.com', passwd='1234567890', image='about:blank')
    await u.save()
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test(loop))
    loop.close()