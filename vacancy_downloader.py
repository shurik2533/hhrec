import httplib
import json
import datetime
import MySQLdb
import ConfigParser
import time
from dateutil.parser import parse

current_time = lambda: int(round(time.time()))
start = current_time()

headers = {"User-Agent": "hh-recommender"}
conn = httplib.HTTPSConnection("api.hh.ru")
per_page = 500
page = 0
count = per_page
while count == per_page:
    path = ("/vacancies?per_page={}&date_from={}&date_to={}&page={}"
            .format(per_page, (datetime.datetime.now() - datetime.timedelta(minutes=5)).strftime('%Y-%m-%dT%H:%M:%S'), 
                    datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S'), page))
    print path

    conn.request("GET", path, headers=headers)
    r1 = conn.getresponse()
    vacancies = r1.read()

    config = ConfigParser.ConfigParser()
    config.readfp(open('my.cfg'))

    db = MySQLdb.connect(host="127.0.0.1", 
                         port=config.getint('mysqld', 'port'), 
                         user=config.get('mysqld', 'user'), 
                         passwd=config.get('mysqld', 'password'), 
                         db=config.get('mysqld', 'database') )
    db.set_character_set('utf8')
    cursor = db.cursor()
    cursor.execute('SET NAMES utf8;')
    cursor.execute('SET CHARACTER SET utf8;')
    cursor.execute('SET character_set_connection=utf8;')

    count = len(json.loads(vacancies)['items'])
    page = page+1
    print count
    cursor = db.cursor()

    for item in json.loads(vacancies)['items']:
        conn = httplib.HTTPSConnection("api.hh.ru")
        conn.request("GET", "/vacancies/{}".format(item['id']), headers=headers)
        r1 = conn.getresponse()
        vacancy = r1.read()
        cursor.execute("""
          INSERT INTO vacancies (id, updated, item) 
          VALUES (%s, %s, %s)
          ON DUPLICATE KEY UPDATE 
            updated  = VALUES(updated), 
            item   = VALUES(item)""", (item['id'], 
                                       parse(item['published_at']).strftime("%Y-%m-%d %H:%M:%S"), 
                                       vacancy))

    db.commit() 
    db.close()
    
print "{} sec".format(current_time()-start)

