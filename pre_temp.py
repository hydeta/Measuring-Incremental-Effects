from pymongo_atlas import MongoAtlasClient
from scitools.db.ibi_impala import *
from investopedia_recirc.investopedia import *
from investopedia_recirc import configuration
import pandas as pd
import pickle as pkl
import string
from gensim import corpora, models, similarities
from gensim.parsing import PorterStemmer
import datetime
import frogress
import numpy as np


class urlObj(object):
    def __init__(self, start_url):
        suffixes = ('.asp', '.aspx')
        # start with either full or relative path
        # strip to get just relative path with no slashes
        if 'www.investopedia.com' in start_url:
            self.mongo = start_url.split('www.investopedia.com/')[1].strip('/')
        else:
            self.mongo = start_url.strip('/')

        # elastic search and mongo corpus both use relative path with no slashes
        self.es = self.mongo
        # impala uses relative path with beginning slash and ending slash if it doesn't end in .asp or .aspx
        self.impala = '/' + self.mongo + '/' if not self.mongo.endswith(suffixes) else '/' + self.mongo
        # most presto queries will be covered by the full path using http and an ending slash
        self.presto = 'http://www.investopedia.com' + self.impala
        # if you want to ensure all results are found include the following 3 versions as well in presto queries
        self.presto_noslash = self.presto.strip('/')
        self.presto_safe = 'https://www.investopedia.com' + self.impala
        self.presto_safe_noslash = self.presto_safe.strip('/')

#list to same pkl files with
list_name = 'goldilocks_total'
# index name for ElasticSearch index
index_name = 'goldilocks_total'

#query impala for URLs
df=pd.read_csv('/Users/thyde/Downloads/goldilocks_recirc_all_subs.csv')
#df1=pd.read_csv('')
relative_path_list = df.relative_path.tolist()
url_objs = [urlObj(url) for url in relative_path_list]
mc = MongoAtlasClient(
    "mongodb://sciences:<PASSWORD>@investopedia-shard-00-00-ibdgj.mongodb.net:27017,investopedia-shard-00-01-ibdgj.mongodb.net:27017,investopedia-shard-00-02-ibdgj.mongodb.net:27017/test?ssl=true&replicaSet=investopedia-shard-0&authSource=admin",
    "sSQXR9fVxNu2P0U5")
my_collection = mc['investopedia']['corpus']
corpus_dict = list(my_collection.find({'url': {'$in': [url.mongo for url in url_objs]}}))
corpus_urls = [i['url'] for i in corpus_dict]
stripped_urls = [i.strip('/') for i in relative_path_list]
output_df = pd.DataFrame({'urls': [i for i in stripped_urls if i not in corpus_urls]})
if len(output_df) > 0:
    print('THERE EXIST URLS NOT IN THE CORPUS, SEE OUTPUT')
    output_df.to_csv('urls_missing_from_mongo_corpus.csv')
else:
    print('All URLS are in the corpus')

def title_sim_construction(url_list, list_name, collection):

    '''
    :param url_list: list of urls to get title similarity for
    :param list_name: list name for purposes of saving pkl file
    :param collection: mongo collection to query
    :return: nothing, just saves files
    '''
    #classic stopwords textfile, will be in repo
    with open('/Users/thyde/Downloads/stopwords.txt') as f:
        stopwords = f.read().split()

    translator = str.maketrans('', '', string.punctuation)
    global_stemmer = PorterStemmer()

    #mc = MongoAtlasClient("mongodb://sciences:<PASSWORD>@investopedia-shard-00-00-ibdgj.mongodb.net:27017,investopedia-shard-00-01-ibdgj.mongodb.net:27017,investopedia-shard-00-02-ibdgj.mongodb.net:27017/test?ssl=true&replicaSet=investopedia-shard-0&authSource=admin", "sSQXR9fVxNu2P0U5")
    #my_collection = mc['investopedia']['corpus']

    docs=list(collection.find({'url': {'$in': url_list}}))

    # pull out title text and create index dictionary
    title_text=[doc['title'] for doc in docs]
    title_ind={title: i for i, title in enumerate(title_text)}

    # parse words in titles
    texts = [[global_stemmer.stem(word) for word in title.translate(translator).lower().split() if word not in stopwords]
         for title in title_text]
    # create gensim corpus
    dictionary = corpora.Dictionary(texts)
    corpus = [dictionary.doc2bow(text) for text in texts]
    tfidf = models.TfidfModel(corpus)
    corpus_tfidf = tfidf[corpus]

    # save files
    pkl.dump(dictionary, open('{}_title_dictionary.pkl'.format(list_name), 'wb'))
    pkl.dump(tfidf, open('{}_title_tfidf.pkl'.format(list_name), 'wb'))
    pkl.dump(corpus_tfidf, open('{}_title_corpus.pkl'.format(list_name),'wb'))
    pkl.dump(title_ind, open('{}_title_ind.pkl'.format(list_name), 'wb'))

def es_index_construction(relative_list, index_name, corpus):
    '''
    :param url_list: list of URLs to get traffic for and upload to ElasticSearch (input relative path)
    :param index_name:
    :param collection:
    :param num_months_traffic:
    :return:
    '''
    def chunks(l, n):
        # For item i in a range that is a length of l,
        for i in range(0, len(l), n):
            # Create an index range for l of n items:
            yield l[i:i+n]

    #iterate through chunks of URLs to get traffic
    appended_data = []
    for url_chunk in frogress.bar(list(chunks(relative_list, 500))):
        urls = format(url_chunk).replace('[', '').replace(']', '')
        query1 = """SELECT relative_path, timelessness, content_type, title, primary_sub_channel, workflow_status,
         avg(entrances) AS avg_organic_entrances_last_3_months
        FROM
          (SELECT content.relative_path,
                 timelessness,
                 content_type,
                 title,
                 content.primary_sub_channel,
                 content.workflow_status,
                 trunc(traffic_date,
                 'mm') AS month, sum(entrances) AS entrances
          FROM investopedia.page_traffic
          LEFT JOIN investopedia.content using(relative_path)
          WHERE content.relative_path IN ({urls})
                  AND traffic_channel = 'organic search'
                  and traffic_date >= '2017-08-01'
                  and traffic_date < '2017-11-01'
          GROUP BY trunc(traffic_date, 'mm'),timelessness, content_type, title, content.relative_path, content.primary_sub_channel, content.workflow_status) traffic
        GROUP BY relative_path, timelessness, content_type, title, primary_sub_channel, workflow_status""".format(urls=urls)

        temp_df = run_ibi_query(query1)
        print(len(temp_df))
        appended_data.append(temp_df)

    traffic_df=pd.concat(appended_data,ignore_index=True)

    traffic_df['url']=traffic_df['relative_path'].apply(lambda x: x.strip('/'))

    #merge corpus with traffic_df to include traffic
    corpus_df=pd.DataFrame.from_dict(corpus)
    merged_df=corpus_df.merge(traffic_df[['url','avg_organic_entrances_last_3_months','primary_sub_channel','workflow_status']],how='left',on='url',copy=False)
    merged_df.rename(columns={'avg_organic_entrances_last_3_months':'avg_entrances','primary_sub_channel':'actual_sub_channel'},inplace=True)
    merged_df['avg_entrances'] = merged_df['avg_entrances'].apply(lambda x: 0 if np.isnan(x) else x)
    merged_df['actual_sub_channel']=merged_df['actual_sub_channel'].fillna('')
    merged_df=merged_df.fillna(0)
    es_corpus=merged_df.to_dict(orient='records')
    #import pdb;pdb.set_trace()

    #select a subset of keys we wish to upload to ElasticSearch
    es_list = [{your_key: dic[your_key] for your_key in
                   ['advertising_channel', 'author', 'avg_entrances', 'actual_sub_channel', 'bodyTEXT', 'channel', 'created', 'sitedate',
                    'sub_advertising_channel', 'sub_channel', 'subtype', 'summary', 'syndate', 'timelessness', 'title',
                    'type', 'updated', 'url', 'workflow_status'] if your_key in dic} for dic in es_corpus]
    for new in es_list:
        if new['syndate'] != None:
            new['syndate'] = datetime.datetime.utcfromtimestamp(int(new['syndate'])).strftime('%Y-%m-%dT%H:%M:%S+00:00')
            new['sitedate'] = datetime.datetime.utcfromtimestamp(int(new['sitedate'])).strftime(
                '%Y-%m-%dT%H:%M:%S+00:00')
    #run configuration from investopedia_recirc to establish connection to ElasticSearch service
    configuration()
    #create index with provided name
    InvestopediaRecirc.create_index(index_name=index_name,force=True,using='default')
    print('index with name: {} created.'.format(index_name))
    #upload subsetted corpus to ES
    InvestopediaRecirc.bulk_update([InvestopediaRecirc(**doc) for doc in es_list])
    print('corpus uploaded to {}.'.format(index_name))

title_sim_construction(url_list=[url.mongo for url in url_objs],list_name=list_name,collection=my_collection)
es_index_construction(relative_list=[url.impala for url in url_objs],index_name=index_name,corpus=corpus_dict)
