import re
import asyncio
import aiohttp
import socket
from aiohttp import ClientSession
from bs4 import BeautifulSoup
import sqlalchemy as sa

# create Tables and API:
db_string = 'postgresql://username:password@localhost:5432/test'
db = sa.create_engine(db_string, encoding='utf8')
metadata = sa.MetaData(db)
pages = sa.Table('pages', metadata,
                sa.Column('id', sa.Integer, primary_key=True),
                sa.Column('link', sa.String(255)),
                sa.Column('request_depth', sa.Integer))
relations = sa.Table('relations', metadata,
                    sa.Column('from_page_id', sa.ForeignKey('pages.id')),
                    sa.Column('link_id', sa.ForeignKey('pages.id')))
with db.connect() as conn:
    conn.execute('DROP TABLE IF EXISTS relations')
    conn.execute('DROP TABLE IF EXISTS pages CASCADE')
    conn.execute('''CREATE TABLE pages (
                                        id serial PRIMARY KEY,
                                        link varchar(255),
                                        request_depth int)''')
    conn.execute('''CREATE TABLE relations (
                                        from_page_id int references pages(id),
                                        link_id int references pages(id))''')
       
def insertPagesTB(URL, depths, db):
    with db.connect() as conn:
        insert_statement = pages.insert().values(link=URL, request_depth=depths)
        conn.execute(insert_statement)
        query_statement = sa.select([sa.func.max(pages.c.id)]).select_from(pages)
        return conn.scalar(query_statement)
        
def insertRelationsTB(parentid, nowid, db):
    with db.connect() as conn:
        conn.execute(relations.insert().values(from_page_id=parentid, link_id=nowid))

# consts:
SEM        = asyncio.Semaphore(1000)
MAX_DEPTHS = 6
DB         = db
START_URL  = "https://en.wikipedia.org/wiki/Quantum_computing"
CONN       = aiohttp.TCPConnector(family=socket.AF_INET,
                                    ssl=False,
                                    )

# fetching:
async def asynchronous(URL, session, depths=0, regExpPattern='/wiki/.*', 
                        parentid=1, max_depths=MAX_DEPTHS, db=DB, sem=SEM):
    if depths <= max_depths:
        nowid = insertPagesTB(URL, depths, db)
        insertRelationsTB(parentid, nowid, db)
        hrefs = []
        async with sem:
            async with session.get(URL) as response:
                response = await response.read()
                page = BeautifulSoup(response)
                for row in page.findAll("a", href=True):
                    if re.fullmatch(regExpPattern, row["href"]):
                        hrefs.append(row["href"])
        tasks = [asyncio.ensure_future(asynchronous(("https://en.wikipedia.org"+i), 
                session=session, depths=depths+1, parentid=nowid)) for i in hrefs]
        await asyncio.gather(*tasks)
            
async def startFetching(URL, conn=CONN):
    async with ClientSession(connector=conn) as session:
        task = [asyncio.ensure_future(asynchronous(URL, session=session))]
        await asyncio.gather(*task)
        
loop = asyncio.get_event_loop()
loop.run_until_complete(startFetching(START_URL))