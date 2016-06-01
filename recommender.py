# -*- coding: utf-8 -*-
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

print 'Start at {}'.format(datetime.datetime.now())
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

def get_recommended(resume_feature, vacancy_features, resume_salary, vacancy_salaries, vacancy_ids, vac_titles, resume_areas, vacancies_area):
    pre_vacancy_features = []
    pre_vacancy_ids = []
    pre_vac_titles = []
    pre_vacancy_salaries = []
    j = 0
    for vac_area in vacancies_area:
        if vac_area in resume_areas:
            pre_vacancy_features.append(vacancy_features[j])
            pre_vacancy_ids.append(vacancy_ids[j])
            pre_vac_titles.append(vac_titles[j])
            pre_vacancy_salaries.append(vacancy_salaries[j])
        j = j+1
        
    new_vacancy_features = []
    new_vacancy_ids = []
    new_vac_titles = []
    if resume_salary == None:
        new_vacancy_features = pre_vacancy_features
        new_vacancy_ids = pre_vacancy_ids
        new_vac_titles = pre_vac_titles
    else:
        i = 0
        for vac_salary in pre_vacancy_salaries:
            if vac_salary == None:
                new_vacancy_features.append(pre_vacancy_features[i])
                new_vacancy_ids.append(pre_vacancy_ids[i])
                new_vac_titles.append(pre_vac_titles[i])
            else:
                min_resume_salary = resume_salary - (resume_salary * 0.2)
                max_resume_salary = resume_salary + (resume_salary * 0.8)
                if vac_salary >= min_resume_salary and vac_salary <= max_resume_salary:
                    new_vacancy_features.append(pre_vacancy_features[i])
                    new_vacancy_ids.append(pre_vacancy_ids[i])
                    new_vac_titles.append(pre_vac_titles[i])
                
            i = i+1    
    
    similarities = []
    ids = []
    titles = []
    if len(new_vacancy_features) > 0:
        c_result = cosine_similarity(resume_feature, new_vacancy_features)
        res = heapq.nlargest(20, range(len(c_result[0])), c_result[0].take)
        
        for j in res:
            similarities.append(c_result[0][j])
            ids.append(new_vacancy_ids[j])
            titles.append(new_vac_titles[j])
    return similarities, ids, titles

resume_features, resume_salaries, resume_ids, resume_areas = get_resumes()

count = 1000
features = get_vacancies(0, count)
features, vacancy_ids, salaries, titles, vacancy_areas = get_vacancies(0, count)

f_len = len(features)

res_similarities = {}
res_recommended_ids = {}
res_recommended_titles = {}
for idx, val in enumerate(resume_features):  
    r_similarities, r_ids, r_titles = get_recommended(resume_features[idx], features, resume_salaries[idx], salaries, vacancy_ids, 
                                                      titles, resume_areas[idx], vacancy_areas)
    res_similarities[resume_ids[idx]] = r_similarities
    res_recommended_ids[resume_ids[idx]] = r_ids
    res_recommended_titles[resume_ids[idx]] = r_titles

i = 0
while f_len > 0:
    features, vacancy_ids, salaries, titles, vacancy_areas = get_vacancies(i*count, count)
    f_len = len(features)
    if f_len > 0:
        for idx, val in enumerate(resume_features):
            r_similarities, r_ids, r_titles = get_recommended(resume_features[idx], features, resume_salaries[idx], salaries, vacancy_ids, 
                                                              titles, resume_areas[idx], vacancy_areas)
            res_similarities[resume_ids[idx]] = res_similarities[resume_ids[idx]] + r_similarities
            res_recommended_ids[resume_ids[idx]] = res_recommended_ids[resume_ids[idx]] + r_ids
            res_recommended_titles[resume_ids[idx]] = res_recommended_titles[resume_ids[idx]] + r_titles
            
    i = i+1
    print 'processed {} vаcancies'.format(i*count)
        
#    if i == 20:
#        break

for resume_id in res_similarities.keys():
    print resume_id
    similarities = res_similarities[resume_id]
    ids = res_recommended_ids[resume_id]
    titles = res_recommended_titles[resume_id]
    max_similarities = heapq.nlargest(20, range(len(numpy.asarray(similarities))), numpy.asarray(similarities).take)
    cursor = db.cursor()
    try:
        cursor.execute("""UPDATE recommendations SET is_active=0 WHERE resume_id='{}'""".format(resume_id))
    except BaseException:
        db.rollback()
    finally:
        cursor.close()
    for ind in max_similarities:
        cursor = db.cursor()
        try:
            cursor.execute("""INSERT INTO recommendations (resume_id, vacancy_id, updated, is_active, similarity, vacancy_title) VALUES ('{}', {}, now(), 1, {}, '{}')""".format(resume_id, ids[ind], similarities[ind], titles[ind].encode('utf-8').strip()))
        except BaseException:
            db.rollback()
        finally:
            cursor.close()
        print 'for {} similarity is {}'.format(ids[ind], similarities[ind])
    db.commit()
        
db.commit()
db.close()

print 'total time {} sec\n'.format(time.time()-start_time)
