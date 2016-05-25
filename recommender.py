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

stemmer = Stemmer.Stemmer('russian')
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
cursor = db.cursor()
cursor.execute("""SELECT item, id FROM vacancies WHERE updated >= (NOW() - INTERVAL 3 DAY)""")

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
    
features_by_type = {}
for employment_type in dictionaries_json['employment']:
    features_by_type[employment_type['id']] = []

spec_ids = pickle.load( open( "spec_ids.p", "rb" ) )
key_skills = pickle.load( open( "key_skills.p", "rb" ) )
title_words = pickle.load( open( "title_words.p", "rb" ) )

count_vectorizer = pickle.load( open( "count_vectorizer.p", "rb" ) )
tfidf_transformer = pickle.load( open( "tfidf_transformer.p", "rb" ) )

data = []
for item in cursor:
    feature = []
    vacancy = json.loads(item[0])
    data.append(vacancy['name'])
    
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
    
    p_doc = p_doc + " " + p_title + " " + p_skills
    
    feature_p_doc = count_vectorizer.transform([p_doc])
    tfidf_feature_p_doc = tfidf_transformer.transform(feature_p_doc)
    
    if features_by_type[vacancy['employment']['id']] == None:
        features_by_type[vacancy['employment']['id']] = []
    features_by_type[vacancy['employment']['id']].append(tfidf_feature_p_doc.toarray()[0])

cursor.close()

db.close()

print('vacancies done')



headers = {"User-Agent": "hh-recommender", "Authorization" : "Bearer T5MIT6GVV85LSVR75CB7U768TR3PFGS990I3QJNFV6A4CBQJF6M30G0MOT8U2V8I"}
conn = httplib.HTTPSConnection("api.hh.ru")
conn.request("GET", "https://api.hh.ru/resumes/mine", headers=headers)
r1 = conn.getresponse()
me = r1.read()
me_json = json.loads(me)
print me_json['items'][0]['id']

conn = httplib.HTTPSConnection("api.hh.ru")
conn.request("GET", "https://api.hh.ru/resumes/{}".format(me_json['items'][0]['id']), headers=headers)
r1 = conn.getresponse()
resume = r1.read()
resume_json = json.loads(resume)

feature = []
resume_type = 'full'

resume_type = resume_json['employment']['id']

#description
p_doc = ''
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

p_doc = p_doc + " " + p_title + " " + p_skills

feature_p_doc = count_vectorizer.transform([p_doc])
feature = tfidf_transformer.transform(feature_p_doc)

features = features_by_type[resume_type]

result = cosine_similarity(feature, features)
res = heapq.nlargest(10, range(len(result[0])), result[0].take)
for i in res:
    print result[0][i]
    print data[i]
