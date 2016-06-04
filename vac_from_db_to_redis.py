# -*- coding: utf-8 -*-
# save all vacancies into redis
import pickle
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_selection import VarianceThreshold
from scipy import spatial
from sklearn.metrics.pairwise import cosine_similarity
import heapq
import numpy
from tinydb import TinyDB
import ConfigParser
import MySQLdb
import json
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_selection import VarianceThreshold
from scipy import spatial
from sklearn.metrics.pairwise import cosine_similarity
import heapq
import numpy
import httplib
import re 
import Stemmer
import time
import datetime
import redis

print 'Start at {}'.format(datetime.datetime.now())
r = redis.StrictRedis(host='localhost', port=6379, db=0)
start_time = time.time()
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

def get_vacancies(offset, rows):
    features = []

    stemmer = Stemmer.Stemmer('russian')
    cursor = db.cursor()
    #будет задвоение, когда во время выборки в несколько запросом добавляются новые данные
    cursor.execute("""SELECT item, id FROM vacancies WHERE updated >= (NOW() - INTERVAL 7 DAY) LIMIT {}, {}""".format(offset, rows))
    vacancy_ids = []
    salaries = []
    cities = []
    titles = []
    areas = []
    for item in cursor:
        feature = []
        vacancy = json.loads(item[0])
        vacancy_ids.append(vacancy['id'])

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
                
        titles.append(vacancy['name'])

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
        salaries.append(salary)
        try:
            areas.append(areas_map[vacancy['area']['id']])
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
            areas.append(missed_area_json['parent_id'])

        p_doc = p_doc + " " + p_title + " " + p_skills
        

        feature_p_doc = count_vectorizer.transform([p_doc])
        tfidf_feature_p_doc = tfidf_transformer.transform(feature_p_doc)
            
        features.append(tfidf_feature_p_doc.toarray()[0])

    cursor.close()
    return features, vacancy_ids, salaries, titles, areas

timeout = 6*24*60*60
vac_cnt = 2000

features, vacancy_ids, salaries, titles, areas = get_vacancies(0, vac_cnt)
cnt = len(features)
for idx, val in enumerate(features): 
    data = {}
    data['features'] = json.dumps(features[idx].tolist()).encode("zlib")
    data['salary'] = salaries[idx]
    data['area'] = areas[idx]
    r.hmset(vacancy_ids[idx], data)
    r.expire(vacancy_ids[idx], timeout)
    
i = 0
while cnt > 0:
    features, vacancy_ids, salaries, titles, areas = get_vacancies(i*vac_cnt, vac_cnt)
    cnt = len(features)
    for idx, val in enumerate(features): 
        data = {}
        data['features'] = json.dumps(features[idx].tolist()).encode("zlib")
        data['salary'] = salaries[idx]
        data['area'] = areas[idx]
        r.hmset(vacancy_ids[idx], data)
        r.expire(vacancy_ids[idx], timeout)
    print 'loaded {}'.format(i*vac_cnt+vac_cnt)
    i = i+1

    
print "finish  at {} min".format((time.time()-start_time)/60.0)

