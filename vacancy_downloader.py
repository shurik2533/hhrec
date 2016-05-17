import httplib
import json
import datetime
import MySQLdb
from dateutil.parser import parse
import ConfigParser

headers = {"User-Agent": "hh-recommender"}
conn = httplib.HTTPSConnection("api.hh.ru")
print ("/vacancies?per_page=500&date_from={}&date_to={}"
       .format((datetime.datetime.now() - datetime.timedelta(minutes=50)).strftime('%Y-%m-%dT%H:%M:%S'), 
               datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')))

conn.request("GET", "/vacancies?per_page=500&date_from={}&date_to={}"
             .format((datetime.datetime.now() - datetime.timedelta(minutes=3)).strftime('%Y-%m-%dT%H:%M:%S'), 
                     datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')), headers=headers)
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

print len(json.loads(vacancies)['items'])
cursor = db.cursor()

for item in json.loads(vacancies)['items']:
    print parse(item['published_at'])
    cursor.execute("""
      INSERT INTO vacancies (id, updated, item) 
      VALUES (%s, %s, %s)
      ON DUPLICATE KEY UPDATE 
        updated  = VALUES(updated), 
        item   = VALUES(item)""", (item['id'], 
                                   parse(item['published_at']).strftime("%Y-%m-%d %H:%M:%S"), 
                                   json.dumps(item)))

db.commit() 
db.close()

