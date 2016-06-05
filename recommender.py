# -*- coding: utf-8 -*-
#recommender2
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
import threading
from multiprocessing import Pool

print 'Start at {}'.format(datetime.datetime.now())
start_time = time.time()
r = redis.StrictRedis(host='localhost', port=6379, db=0)
config = ConfigParser.ConfigParser()
config.readfp(open('my.cfg'))
db = MySQLdb.connect(host="127.0.0.1", 
                     port=config.getint('mysqld', 'port'), 
                     user=config.get('mysqld', 'user'), 
                     passwd=config.get('mysqld', 'password'), 
                     db=config.get('mysqld', 'database') )
db.autocommit(True)
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

def get_resumes():
    salaries = []
    features = []
    ids = []
    areas = []
    stemmer = Stemmer.Stemmer('russian')
    cursor = db.cursor()
    cursor.execute("""SELECT item FROM resumes WHERE is_active=1""")
    for item in cursor:
        resume_json = json.loads(item[0])
        feature = []
        #description
        p_doc = ''
        if resume_json['skills'] != None:
            doc = re.sub('<[^>]*>', '', resume_json['skills'].lower())
            doc = re.sub('&quot;', '', doc)
            doc = re.sub(ur'[^a-zа-я]+', ' ', doc, re.UNICODE)
            words = re.split(r'\s{1,}', doc.strip())
            for word in words:
                word = stemmer.stemWord(word.strip())
                if len(word.strip()) > 1:
                    p_doc = p_doc + " " + word

        #title
        p_title = ''
        if resume_json['title'] != None:
            title = re.sub(ur'[^a-zа-я]+', ' ', resume_json['title'].lower(), re.UNICODE)
            words = re.split(r'\s{1,}', title.strip())
            for title_word in words:
                title_word = stemmer.stemWord(title_word)
                if len(title_word.strip()) > 1:
                    p_title = p_title + " " + title_word.strip()

        #keyskills
        p_skills = ''
        res_skills = resume_json['skill_set']
        for skill in res_skills:
            words = re.split(r'\s{1,}', skill.lower().strip())
            for word in words:
                word = stemmer.stemWord(word)
                if len(word.strip()) > 1:
                    p_skills = p_skills + " " + word.strip()

        #salary
        salary = None
        if resume_json['salary'] != None and resume_json['salary']['amount'] != None:
            salary = resume_json['salary']['amount']/currency_rates[resume_json['salary']['currency']]
        max_salary = 500000.0
        if salary >= max_salary:
            salary = max_salary
        
        
        res_areas = []
        if resume_json['area'] == None:
            res_areas.append(areas_map["1"])
        else :
            res_areas.append(areas_map[resume_json['area']['id']])
        for area in resume_json['relocation']['area']:
            res_areas.append(areas_map[area['id']])
        areas.append(res_areas)
        

        p_doc = p_doc + " " + p_title + " " + p_skills
        feature_p_doc = count_vectorizer.transform([p_doc])
        feature = tfidf_transformer.transform(feature_p_doc)
        features.append(feature.toarray())
        salaries.append(salary)
        ids.append(resume_json['id'])
    cursor.close()
    return features, salaries, ids, areas

resume_features, resume_salaries, resume_ids, resume_areas = get_resumes()
lock = threading.Lock()

def process_vacancy_ids(vacancies):
    pre_vacancy_similarities = {}
    pre_vacancy_ids = {}

    for idx, val in enumerate(resume_features):
        new_vacancy_features = []
        new_vacancy_ids = []
        for vac_id, vac_data in vacancies.iteritems():
            if resume_areas[idx][0] == vac_data['area'] and (resume_salaries[idx] == None or vac_data['salary'] == 'None'):
                new_vacancy_features.append(json.loads(vac_data['features'].decode('zlib')))
                new_vacancy_ids.append(vac_id)
            elif resume_areas[idx][0] == vac_data['area']:
                min_resume_salary = resume_salaries[idx] - (resume_salaries[idx] * 0.2)
                max_resume_salary = resume_salaries[idx] + (resume_salaries[idx] * 0.8)
                vac_salary = float(vac_data['salary'])
                if vac_salary >= min_resume_salary and vac_salary <= max_resume_salary:
                    new_vacancy_features.append(json.loads(vac_data['features'].decode('zlib')))
                    new_vacancy_ids.append(vac_id)
                    
        similarities = []
        ids = []
        if len(new_vacancy_features) > 0:
            c_result = cosine_similarity(resume_features[idx], new_vacancy_features)
            res = heapq.nlargest(20, range(len(c_result[0])), c_result[0].take)

            for j in res:
                similarities.append(c_result[0][j])
                ids.append(new_vacancy_ids[j])
        
        lock.acquire()
        try:
            if resume_ids[idx] not in pre_vacancy_similarities:
                pre_vacancy_similarities[resume_ids[idx]] = similarities
                pre_vacancy_ids[resume_ids[idx]] = ids
            else:
                pre_vacancy_similarities[resume_ids[idx]] = pre_vacancy_similarities[resume_ids[idx]] + similarities
                pre_vacancy_ids[resume_ids[idx]] = pre_vacancy_ids[resume_ids[idx]] + ids
        finally:
            lock.release()
            
    return len(vacancies), pre_vacancy_similarities, pre_vacancy_ids

tp_res = [] 
tpool = Pool(3) 
def iterate_ids(start):
    cnt = 500
    rcursor = r.scan(cursor=start, count=cnt)
    vacancies = {}
    for vac_id in rcursor[1]:
        vacancies[vac_id] = r.hgetall(vac_id)
    tres = tpool.apply_async(process_vacancy_ids, (vacancies,))
    tp_res.append(tres)
    while (rcursor[0] != 0):
        rcursor = r.scan(cursor=rcursor[0], count=cnt)
        vacancies = {}
        for vac_id in rcursor[1]:
            vacancies[vac_id] = r.hgetall(vac_id)
        tres = tpool.apply_async(process_vacancy_ids, (vacancies,))
        tp_res.append(tres)

iterate_ids(0)

c = 0
pre_vacancy_similarities = {}
pre_vacancy_ids = {}
for tr in tp_res:
    cnt, p_vacancy_similarities, p_vacancy_ids = tr.get()
    for resume_id in p_vacancy_similarities.keys():
        if resume_id not in pre_vacancy_similarities:
            pre_vacancy_similarities[resume_id] = p_vacancy_similarities[resume_id]
            pre_vacancy_ids[resume_id] = p_vacancy_ids[resume_id]
        else:
            pre_vacancy_similarities[resume_id] = pre_vacancy_similarities[resume_id]+p_vacancy_similarities[resume_id]
            pre_vacancy_ids[resume_id] = pre_vacancy_ids[resume_id]+p_vacancy_ids[resume_id]
    
    c = c+cnt
    print 'processed {}'.format(c)

def finalize_recommendations(resume_id):
    result = []
    similarities = pre_vacancy_similarities[resume_id]
    ids = pre_vacancy_ids[resume_id]
    max_similarities = heapq.nlargest(20, range(len(numpy.asarray(similarities))), numpy.asarray(similarities).take)
    lock.acquire()
    try:
        cursor = db.cursor()
        try:
            cursor.execute("""UPDATE recommendations SET is_active=0 WHERE resume_id='{}'""".format(resume_id))
        except BaseException as ex:
            print ex
        finally:
            cursor.close()
    finally:
        lock.release()
        
    for ind in max_similarities:
        conn = httplib.HTTPSConnection("api.hh.ru")
        conn.request("GET", "https://api.hh.ru/vacancies/{}".format(ids[ind]), headers=headers)
        r1 = conn.getresponse()
        t_vacancy = r1.read()
        t_vacancy_json = json.loads(t_vacancy)
        try:
            title = t_vacancy_json['name'].encode('utf-8').strip()
        except KeyError as ex:
            print ex
            title = 'Title temporary not found'

        lock.acquire()
        try:
            cursor = db.cursor()
            try:
                cursor.execute("""INSERT INTO recommendations (resume_id, vacancy_id, updated, is_active, similarity, vacancy_title) VALUES ('{}', {}, now(), 1, {}, '{}')""".format(resume_id, ids[ind], similarities[ind], title))
            except BaseException as err:
                print err
            finally:
                cursor.close()
        finally:
            lock.release()
        result.append('{}. for {} similarity is {}'.format(resume_id, ids[ind], similarities[ind]))

    return result

p_res = [] 
pool = Pool(7) 
for resume_id in pre_vacancy_similarities.keys():
    res = pool.apply_async(finalize_recommendations, (resume_id,))
    p_res.append(res)
    
for t in p_res:
    res = t.get()
    for s in res:
        print s
        
db.close()

print 'total time {} sec\n'.format(time.time()-start_time)

