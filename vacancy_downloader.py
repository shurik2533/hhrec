import httplib
import json
import datetime
import MySQLdb
import ConfigParser
import time
import threading
from dateutil.parser import parse

current_time = lambda: int(round(time.time()))
start = current_time()
    
headers = {"User-Agent": "hh-recommender"}

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
cursor.close()

def get_vacancy_ids():
    vacancy_ids = []
    conn = httplib.HTTPSConnection("api.hh.ru")
    per_page = 200
    page = 0
    count = per_page
    date_from = (datetime.datetime.now() - datetime.timedelta(minutes=5)).strftime('%Y-%m-%dT%H:%M:%S')
    date_to = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
    while count == per_page:
        path = ("/vacancies?per_page={}&date_from={}&date_to={}&page={}"
                .format(per_page, date_from, date_to, page))
        print path

        conn.request("GET", path, headers=headers)
        r1 = conn.getresponse()
        vacancies = r1.read()

        count = len(json.loads(vacancies)['items'])
        page = page+1
        for item in json.loads(vacancies)['items']:
            vacancy_ids.append(item['id'])
    return vacancy_ids
        

def process_vacancies(vacancy_ids):
    headers = {"User-Agent": "hh-recommender"}

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
    cursor.close()

    for vac_id in vacancy_ids:
        conn = httplib.HTTPSConnection("api.hh.ru")
        conn.request("GET", "/vacancies/{}".format(vac_id), headers=headers)
        r1 = conn.getresponse()
        vacancy = r1.read()
        vacancy_json = json.loads(vacancy)
        cursor = db.cursor()
        cursor.execute("""
          INSERT INTO vacancies (id, updated, item) 
          VALUES (%s, %s, %s)
          ON DUPLICATE KEY UPDATE 
            updated  = VALUES(updated), 
            item   = VALUES(item)""", (vac_id, 
                                       parse(vacancy_json['published_at']).strftime("%Y-%m-%d %H:%M:%S"), 
                                       vacancy))
        db.commit() 
        cursor.close()
    db.close()

ids = get_vacancy_ids()
print len(ids)
vac_id_chunks=[ids[x:x+100] for x in xrange(0, len(ids), 100)]
t_num = 1;
threads = []
for vac_id_chunk in vac_id_chunks:
    print 'starting t{}'.format(t_num)
    t_num = t_num + 1
    t = threading.Thread(target=process_vacancies, kwargs={'vacancy_ids': vac_id_chunk})
    threads.append(t)
    t.start()
    
for t in threads:
    t.join()
    

db.close()
print "{} sec".format(current_time()-start)
