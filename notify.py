import asyncio
import asyncpg
import queue
import yaml
import importlib
import sys
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers
import logging


async def run(loop, config):
    pgconf = config["postgresql"]
    pgcon = await asyncpg.connect(host=pgconf["host"], port=pgconf["port"],
                                  user=pgconf["user"], password=pgconf["password"],
                                  database=pgconf["database"], loop=loop)
    logging.info("Connected to database")
    ncon = NATS()
    nconf = config["nats"]
    await ncon.connect(f'nats://{nconf["host"]}:{nconf["port"]}', loop=loop)
    logging.info("Connected to NATS")
    notifq = asyncio.Queue()

    def listener(*args):
        notifq.put_nowait(args)
        logging.info(f'new notification : {args[2]}')
    for channel in pgconf["channels"]:
        await pgcon.add_listener(channel, listener)
        logging.info("Listening to async notification")

    async def worker():
        while True:
            notm = await notifq.get()
            topic, payload = (notm[2], notm[3])
            await ncon.publish(f'{nconf["subjectPrefix"]}.{topic}', bytes(payload,"utf-8"))
            notifq.task_done()
    task = asyncio.ensure_future(worker())
    await task

if __name__ == "__main__":
    if(len(sys.argv) < 2):
        logging.error("Config file not supplied")
        sys.exit()
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',level=logging.DEBUG)
    config = yaml.load(open(sys.argv[1], 'rb'))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop, config))
