import asyncio
import asyncpg
import queue
import yaml
import importlib
import sys
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers
from jsonpath_rw import jsonpath, parse
import logging
import json
from streamz import Source


def dblog(con,msg):
    logging.info(msg)

async def run(loop,opt):
    pgconf = opt["config"]["postgresql"]
    pgcon = await asyncpg.connect(host=pgconf["host"], port=pgconf["port"],
                                  user=pgconf["user"], password=pgconf["password"],
                                  database=pgconf["database"], loop=loop)
    logging.info("Connected to database")
    mapping = opt["config"]["mapping"]    
    
    pgcon.add_log_listener(dblog)
    async def inserter(msg):
        
        logging.debug("New message")
        data = json.loads(msg.data)
        matchs = [parse(path).find(data) for path in mapping["values"]]
        values = [None if len(match) == 0 else match[0].value for match in matchs]        
        #sql = "insert into tracking.test1 (assetId,location,time) values($1,$2,$3)"
        sql = mapping["sql"]
        try:
            if("dry-run" in opt):
                logging.info(f'{sql} , {tuple(values)}')
                res = "INSERT 0 1"
            else:
                res = await pgcon.execute(sql,*values)
        except:
            logging.error(f'Exception message : {sys.exc_info()[1]}')
        if(res != "INSERT 0 1"):
            logging.warning("Insert failed")
        else:
            logging.info("Insert succed")

    ncon = NATS()
    nconf = opt["config"]["nats"]
    await ncon.connect(f'nats://{nconf["host"]}:{nconf["port"]}', loop=loop)
    logging.info("Connected to NATS")
    await ncon.subscribe(nconf["subject"], cb=inserter)
    logging.info("Subscribed to NATS")

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',level=logging.DEBUG)
    opt = dict()
    if(len(sys.argv) < 2):
        logging.error("Config file not supplied")
        sys.exit()
    elif(len(sys.argv) > 2):
        if(sys.argv[2] == "dry-run"):
            opt["dry-run"] = True
    config = yaml.load(open(sys.argv[1], 'rb'))
    opt["config"] = config
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop,opt))
    loop.run_forever()
    
