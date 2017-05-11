import www.orm as orm
import asyncio
from www.models import User, Blog, Comment

async def test(loop):

    await orm.create_pool(loop=loop,user='root', password='', db='awesome')
    u = User(name='lyh1', email='lyh1@example.com', passwd='0987654321', image='about:blank')
    await u.save()

loop = asyncio.get_event_loop()
loop.run_until_complete(test(loop))
loop.close()