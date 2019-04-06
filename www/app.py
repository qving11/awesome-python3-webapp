#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__author__ = 'Michael Liao'

import logging; logging.basicConfig(level=logging.INFO)

import asyncio, os, json, time
from datetime import datetime

from aiohttp import web

def index(request):	#what if request the index
    return web.Response(body=b'Awesome', content_type='text/html', charset='UTF-8')

#"@asyncio.coroutine" could be replaced by "async def" meantime "yield" be replaced by "await"
async def init(loop):
    app = web.Application(loop=loop)
    app.router.add_route('GET', '/', index)
    srv = await loop.create_server(app.make_handler(), '127.0.0.1', 8888)
    logging.info('server started at http://127.0.0.1:8888...')
    return srv

loop = asyncio.get_event_loop()
loop.run_until_complete(init(loop))
loop.run_forever()