# -*- coding: utf-8 -*-
#vacancy downloader 2
import httplib
import json
import MySQLdb
import ConfigParser
import time
import threading
from dateutil.parser import parse
import pickle
from sklearn.metrics.pairwise import cosine_similarity
from tinydb import TinyDB
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_selection import VarianceThreshold
from scipy import spatial
from sklearn.metrics.pairwise import cosine_similarity
import heapq
import numpy
import re 
import Stemmer
import datetime
import redis


print 'Start at {}'.format(datetime.datetime.now())
r = redis.StrictRedis(host='localhost', port=6379, db=0)
start_time = time.time()
timeout = 5*24*60*60
headers = {"User-Agent": "hh-recommender"}
conn = httplib.HTTPSConnection("api.hh.ru")
conn.request("GET", "https://api.hh.ru/dictionaries", headers=headers)
r1 = conn.getresponse()
dictionaries = r1.read()
dictionaries_json = json.loads(dictionaries)

currencies = dictionaries_json['currency']
currency_rates = {}
for currency in currencies:
    currency_rates[currency['code']] = currency['rate']
    
#areas
conn = httplib.HTTPSConnection("api.hh.ru")
conn.request("GET", "https://api.hh.ru/areas", headers=headers)
r1 = conn.getresponse()
areas = r1.read()
areas_json = json.loads(areas)
areas_map = {}
def build_areas_map(areas, areas_map):
    for area in areas:
        if area['id'] == '1':#msk
            parent_id = '2019'
        elif area['id'] == '2':#spb
            parent_id = '145'
        elif area['id'] == '115':#kiev
            parent_id = '2164'
        elif area['id'] == '1002':#minsk
            parent_id = '2237'
        else:
            parent_id = area['parent_id']
        areas_map[area['id']] = parent_id
        build_areas_map(area['areas'], areas_map)
        
build_areas_map(areas_json, areas_map)
    
spec_ids = pickle.load( open( "spec_ids.p", "rb" ) )
key_skills = pickle.load( open( "key_skills.p", "rb" ) )
title_words = pickle.load( open( "title_words.p", "rb" ) )

count_vectorizer = pickle.load( open( "count_vectorizer.p", "rb" ) )
tfidf_transformer = pickle.load( open( "tfidf_transformer.p", "rb" ) )
stemmer = Stemmer.Stemmer('russian')
    
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
    for vac_id in vacancy_ids:
        conn = httplib.HTTPSConnection("api.hh.ru")
        conn.request("GET", "/vacancies/{}".format(vac_id), headers=headers)
        r1 = conn.getresponse()
        vacancy_txt = r1.read()
        vacancy = json.loads(vacancy_txt)
        
        feature = []

        #description
        p_doc = ''
        doc = re.sub('<[^>]*>', '', vacancy['description'].lower())
        doc = re.sub('&quot;', '', doc)
        doc = re.sub(ur'[^a-zа-я]+', ' ', doc, re.UNICODE)
        words = re.split(r'\s{1,}', doc.strip())
        for word in words:
            word = stemmer.stemWord(word.strip())
            if len(word.strip()) > 1:
                p_doc = p_doc + " " + word

        #title
        p_title = ''
        title = re.sub(ur'[^a-zа-я]+', ' ', vacancy['name'].lower(), re.UNICODE)
        words = re.split(r'\s{1,}', title.strip())
        for title_word in words:
            title_word = stemmer.stemWord(title_word)
            if len(title_word.strip()) > 1:
                p_title = p_title + " " + title_word.strip()

        #keyskills
        p_skills = ''
        vac_skills = vacancy['key_skills']
        for skill in vac_skills:
            words = re.split(r'\s{1,}', skill['name'].lower().strip())
            for word in words:
                word = stemmer.stemWord(word)
                if len(word.strip()) > 1:
                    p_skills = p_skills + " " + word.strip()
                    
        #salary
        salary = None
        if vacancy['salary'] != None:
            if vacancy['salary']['from'] == None and vacancy['salary']['to'] != None:
                salary = vacancy['salary']['to']/currency_rates[vacancy['salary']['currency']]
            elif vacancy['salary']['to'] == None and vacancy['salary']['from'] != None:
                salary = vacancy['salary']['from']/currency_rates[vacancy['salary']['currency']]
            elif vacancy['salary']['to'] != None and vacancy['salary']['from'] != None:
                salary = ((vacancy['salary']['from'] + vacancy['salary']['to'])/2)/currency_rates[vacancy['salary']['currency']]
        max_salary = 500000.0
        if salary >= max_salary:
            salary = max_salary

        try:
            area_id = areas_map[vacancy['area']['id']]
        except KeyError:
            print 'missed area id {} for vacancy {}'.format(vacancy['area']['id'], vacancy['id'])
            
            conn = httplib.HTTPSConnection("api.hh.ru")
            conn.request("GET", "https://api.hh.ru/vacancies/{}".format(vacancy['id']), headers=headers)
            r1 = conn.getresponse()
            missed_area_vacancy = r1.read()
            missed_area_vacancy_json = json.loads(missed_area_vacancy)
            
            conn = httplib.HTTPSConnection("api.hh.ru")
            conn.request("GET", "https://api.hh.ru/areas/{}"
                         .format(missed_area_vacancy_json['area']['id']), headers=headers)
            r1 = conn.getresponse()
            missed_area = r1.read()
            missed_area_json = json.loads(missed_area)
            areas_map[vacancy['area']['id']] = missed_area_json['parent_id']
            area_id = missed_area_json['parent_id']

        p_doc = p_doc + " " + p_title + " " + p_skills
        

        feature_p_doc = count_vectorizer.transform([p_doc])
        tfidf_feature_p_doc = tfidf_transformer.transform(feature_p_doc)
            
        data = {}
        data['features'] = json.dumps(tfidf_feature_p_doc.toarray()[0].tolist()).encode("zlib")
        data['salary'] = salary
        data['area'] = area_id
        r.hmset(vacancy['id'], data)
        r.expire(vacancy['id'], timeout)


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
print "{} sec".format(time.time() - start_time)
