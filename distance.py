from geopy import distance
import asyncio
import yaml
import importlib
from streamz import Stream
import functools
import json
import sys
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers


async def run(loop, config):
    ncon = NATS()
    nconf = config["nats"]
    await ncon.connect(f'nats://{nconf["host"]}:{nconf["port"]}', loop=loop)

    async def getdistance(state, data):
        loc = state.get(data["asset_id"])
        if(loc == None):
            return None
        else:
            d = distance.distance((loc["latitude"], loc["longitude"]),
                                  (data["latitude"], data["longitude"])).meters
        return dict(distance=d,
                    asset_id=data["id"],
                    timestamp=data["timestamp"])

    source = Stream()
    source.map(lambda msg: json.loads(msg)).\
        map(lambda msg: dict(asset_id=msg["clientId"],
                             latitude=msg["payload"]["latitude"],
                             longitude=msg["payload"]["longitude"],
                             timestamp=msg["payload"]["timestamp"])).\
        accumulate(getdistance, start=dict()).\
        filter()


    await ncon.subscribe(nconf["input"], cb=source.emit)

config = yaml.load(open('distance.yml', 'rb'))
loop = asyncio.get_event_loop()
loop.run_until_complete(run(loop, config))
