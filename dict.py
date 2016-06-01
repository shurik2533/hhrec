import json
from tinydb import TinyDB
import pickle
import Stemmer
import re 
import pickle
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_selection import VarianceThreshold
from scipy import spatial
from sklearn.metrics.pairwise import cosine_similarity
import heapq
import numpy

db = TinyDB('vacancies.json')
db2 = TinyDB('vacancies2.json')
vacancies1 = db.all()
vacancies2 = db2.all()
vacancies = vacancies1+vacancies2

# db = TinyDB('vacancies.json')
# vacancies = db.all()

stemmer = Stemmer.Stemmer('russian')

vac_by_spec = {}
i = 0
#keyskills, title, description
for vacancy in vacancies:
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
    
    for spec in vacancy['specializations']:
        if spec['id'] not in vac_by_spec:
            vac_by_spec[spec['id']] = []
        vac_by_spec[spec['id']].append(p_doc)
    
#     i = i+1
#     if i > 100:
#         break
print 'docs processed'
words = set()
for key in vac_by_spec:
    corpus = vac_by_spec[key]
    vectorizer = CountVectorizer(min_df=1)
    X = vectorizer.fit_transform(corpus)
    transformer = TfidfTransformer()
    X_tfidf = transformer.fit_transform(X)
    spec_means = X_tfidf.mean(axis=0)
    spec_means_arr = numpy.squeeze(numpy.asarray(spec_means))
    res = heapq.nlargest(350, range(len(spec_means_arr)), spec_means_arr.take)
    for i in res:
        words.add(vectorizer.get_feature_names()[i])
        
print len(words)
pickle.dump( words, open( "spec_dict2.p", "wb" ) )
print 'finish'
