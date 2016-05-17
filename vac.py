import httplib
import json
import pickle
from tinydb import TinyDB
headers = {"User-Agent": "hh-recommender"}

db = TinyDB('/home/shurik2533/vacancies2.json')
k = 0
for i in range(16748000, 0, -1): #18822744
    conn = httplib.HTTPSConnection("api.hh.ru")
    conn.request("GET", "/vacancies/{0}".format(i), headers=headers)
    r1 = conn.getresponse()
    k = k+1
    if r1.status==200:
        vacancy_data = r1.read()
        if json.loads(vacancy_data)['archived'] == False:
            db.insert(json.loads(vacancy_data))
            k=0
    if i%1000 == 0:
        print i
    if k == 30000:
        print "break"
        break;
