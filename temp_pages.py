import pandas as pd
import os.path
import numpy as np
import frogress
from scitools.db.cassandra_db import *
from scitools.db.elasticsearch_db import *
from scitools.db.presto import *
from scitools.db.ibi_impala import *
from scitools.moz import *
from elasticsearch_dsl import Q, Search
from elasticsearch_dsl.query import Bool
from spacy.strings import StringStore
import scipy.sparse as sp
from scipy.spatial.distance import cdist
from gensim import corpora, models, similarities
from gensim.parsing import PorterStemmer
import string
import itertools


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


def create_mlt_query_body(url, boosts_wanted=['body.bigrams','body.unigrams','title.bigrams','title.unigrams'],
                          es_index='recirc_loserlist', body_bigram_boost=4, body_unigram_boost=1,
                          title_bigram_boost=4, title_unigram_boost=1, min_should_match=1, min_doc_freq=0,
                          min_term_freq=0, max_query_terms=50):
    '''
    create the elastic search query
    :param url: urlObj of url to search for
    :param boosts_wanted: list of which title/body uni/bigram boosts to use
    :param es_index: index name
    :param body_bigram_boost:
    :param body_unigram_boost:
    :param title_bigram_boost:
    :param title_unigram_boost:
    :param min_should_match:
    :param min_doc_freq:
    :param min_term_freq:
    :param max_query_terms:
    :return: es query
    '''

    text_boosts = {
        'body.bigrams': dict(boost=body_bigram_boost, minimum_should_match=min_should_match, min_doc_freq=min_doc_freq,
                             min_term_freq=min_term_freq, max_query_terms=max_query_terms, include=True),
        'body.unigrams': dict(boost=body_unigram_boost, minimum_should_match=min_should_match, min_doc_freq=min_doc_freq,
                              min_term_freq=min_term_freq, max_query_terms=max_query_terms, include=True),
        'title.bigrams': dict(boost=title_bigram_boost, minimum_should_match=min_should_match, min_doc_freq=min_doc_freq,
                              min_term_freq=min_term_freq, max_query_terms=max_query_terms, include=True),
        'title.unigrams': dict(boost=title_unigram_boost, minimum_should_match=min_should_match, min_doc_freq=min_doc_freq,
                               min_term_freq=min_term_freq, max_query_terms=max_query_terms, include=True)
    }

    mlt_like = {
        '_index': es_index,
        '_type': 'investopedia_doc',
        '_id': url.es
    }

    mlt = [Q('more_like_this', fields=[field], like=mlt_like, **boost) for field, boost in text_boosts.items()
           if field in boosts_wanted and boost.get('boost') > 0]

    query = Q('bool', should=mlt, minimum_should_match=1, boost=1)

    return query


def batch_related_content(urls, es_index='goldilocks_redirected_correct_subchannels',
                          n_widenet=6, boosts_wanted=['body.bigrams','body.unigrams','title.bigrams','title.unigrams'],
                          body_bigram_boost=4, body_unigram_boost=1,
                          title_bigram_boost=4, title_unigram_boost=1, min_should_match=1, min_doc_freq=0,
                          min_term_freq=0, max_query_terms=50):
    '''
    get related content for a list of urls
    :param urls: list of urlObjs
    :param es_index: index name
    :param n_widenet: number of similar urls to return
    :param boosts_wanted: list of which title/body uni/bigram boosts to use
    :param body_bigram_boost:
    :param body_unigram_boost:
    :param title_bigram_boost:
    :param title_unigram_boost:
    :param min_should_match:
    :param min_doc_freq:
    :param min_term_freq:
    :param max_query_terms:
    :return: dataframe of recirc results
    '''

    es_connection = aws_es('https://search-investopedia-moat-7ckxpw7t5s3rycpgq6q56p3g2e.us-east-1.es.amazonaws.com')

    batch_search = []

    req_head = {'index': es_index}
    s = Search()

    for url in urls:
        req_body = create_mlt_query_body(url, boosts_wanted=boosts_wanted, es_index=es_index,
                                         body_bigram_boost=body_bigram_boost, body_unigram_boost=body_unigram_boost,
                                         title_bigram_boost=title_bigram_boost, title_unigram_boost=title_unigram_boost,
                                         min_should_match=min_should_match, min_doc_freq=min_doc_freq,
                                         min_term_freq=min_term_freq, max_query_terms=max_query_terms)
        req_body = s.query(Bool(must=req_body)).to_dict()
        req_body['size'] = n_widenet
        batch_search.extend([req_head, req_body])

    res = es_connection.msearch(body=batch_search, request_timeout=280)

    result_dfs = []

    def _parse_source(source_dict, source_name):
        try:
            return source_dict[source_name]
        except KeyError:
            if source_name == 'avg_organic_entrances_last_3_months':
                return 0
            else:
                return ''

    for i, results in enumerate(res['responses']):

        max_score = results['hits']['max_score']
        df = pd.DataFrame(results['hits']['hits'])

        if not df.empty:
            df['text_sim_score'] = df['_score'].div(max_score)
            df['url'] = df['_id'].apply(lambda x: 'http://www.investopedia.com/' + x)
            df['timelessness'] = df['_source'].apply(lambda x: _parse_source(x, 'timelessness'))
            df['title'] = df['_source'].apply(lambda x: _parse_source(x, 'title'))
            df['primary_sub_channel'] = df['_source'].apply(lambda x: _parse_source(x, 'actual_sub_channel'))
            df['workflow_status'] = df['_source'].apply(lambda x: _parse_source(x, 'workflow_status'))
            df['content_type'] = df['_source'].apply(lambda x: _parse_source(x, 'subtype'))
            df['avg_organic_entrances_last_3_months'] = df['_source'].apply(lambda x: _parse_source(x, 'avg_entrances'))
            df['origin'] = urls[i].presto

            result_dfs.append(df[['_id', 'url', 'text_sim_score', 'origin', 'timelessness',
                                  'content_type', 'primary_sub_channel', 'title', 'avg_organic_entrances_last_3_months', 'workflow_status']])
            #result_dfs.append(df[['_id', 'url', 'text_sim_score', 'origin', 'timelessness',
            #                      'content_type', 'title']])

    return pd.concat(result_dfs)


def get_keywords_by_url(urls, connection=None, num_keywords=10, volume_cutoff=None, rank_cutoff=None):
    '''
    Gets top n keywords or high volume and rank keywords for a url by computing estimated clicks
    :param urls: list of url to get keywords against
    :param connection: presto connection
    :param num_keywords: the number of keywords to return or all
    :param volume_cutoff: minimum volume if high volume keywords wanted
    :param rank_cutoff: lowest rank if high rank keywords wanted
    :return: dataframe of urls and keywords
    '''

    if connection is None:
        connection = connect_presto()

    if not isinstance(urls,list):
        urls=[urls]

    # get all versions of url to search for
    orig_list = urls
    urls = [url.presto for url in orig_list]
    urls.extend([url.presto_safe for url in orig_list])
    urls.extend([url.presto_noslash for url in orig_list])
    urls.extend([url.presto_safe_noslash for url in orig_list])
    urls = list(set(urls))

    # format filters for query
    urls = format(urls).replace('[', '').replace(']', '')
    num_filter = 'WHERE rn <= %d'%num_keywords if num_keywords != 'all' else ''
    vol_filter = 'volume_prediction > %d'%volume_cutoff if volume_cutoff else 'volume_prediction > 0'
    rank_filter = 'and ranking <= 10' if rank_cutoff else ''

    query = """WITH
    estimated_clicks as (
    SELECT url,
                 volume_prediction,
                 ranking,
                 volume_prediction / (ranking * ranking + 2) as estimated_keyword_clicks,
                 keyword
        FROM moz_investopedia.serps
         WHERE url in ({urls})
               and {vol_filter} {rank_filter}
    )
    SELECT url,keyword as keywords
       FROM
         (SELECT url, keyword, volume_prediction, estimated_keyword_clicks, ranking,
                 row_number() OVER ( PARTITION BY url
                                    ORDER BY estimated_keyword_clicks desc, ranking asc, volume_prediction desc) AS rn
          FROM estimated_clicks) rankings
       {num_filter}""".format(num_filter=num_filter, urls=urls, vol_filter=vol_filter, rank_filter=rank_filter)

    top_keywords = pd.read_sql(query, connection)

    return top_keywords


def get_serp_by_keyword(keywords, connection=None):
    '''
    get serps for a list of keywords
    :param keywords: list of keywords
    :param connection: presto connection
    :return: dataframe with keyword, volume, and list of urls in serp
    '''
    if connection is None:
        connection = connect_presto()

    # format keyword list for query
    keywords = str(tuple([''.join([let if let != "'" else "''" for let in word]) for word in keywords])).replace('"', "'")

    query = '''SELECT keyword, ranking, url, volume_prediction FROM moz_investopedia.serps
                where keyword in {}
                order by keyword, ranking'''.format(keywords)

    df = pd.read_sql(query, connection)

    # group urls into a list and append volume
    serp_df = df.groupby('keyword')['url'].apply(list).to_frame().reset_index().merge(df.groupby('keyword')['volume_prediction'].first().to_frame().reset_index(), on='keyword')
    return serp_df


def get_traffic_by_url(urls):
    """
    Get average of last num_months (default=3) months organic entrances for a list of urls
    :param urls: list of urls
    :param num_months_traffic: number of months to average traffic over
    :return traffic df
    """
    urls = format(urls).replace('[', '').replace(']', '')

    query = """SELECT relative_path, primary_sub_channel,
         avg(entrances) AS avg_organic_entrances_last_3_months
        FROM
          (SELECT content.relative_path,
                 timelessness,
                 content_type,
                 title,
                 content.primary_sub_channel,
                 trunc(traffic_date,
                 'mm') AS month, sum(entrances) AS entrances
          FROM investopedia.page_traffic
          LEFT JOIN investopedia.content using(relative_path)
          WHERE content.relative_path IN ({urls})
                  and traffic_date >= '2017-08-01'
                  and traffic_date < '2017-11-01'
          GROUP BY trunc(traffic_date, 'mm'),timelessness, content_type, title, content.relative_path, content.primary_sub_channel) traffic
        GROUP BY relative_path, primary_sub_channel""".format(urls=urls)

    traffic_df = run_ibi_query(query)
    traffic_df['url'] = traffic_df['relative_path'].apply(lambda x: urlObj(x).presto)

    return traffic_df


def get_distinct_urls_in_serps(serp_df, kws, n_serp_urls=10):
    """
    Helper function to get the top n urls from each serp in a list of keywords
    :param serp_df: df of serps for all keywords
    :param kws: list of keywords
    :param n_serp_urls: number of urls deep to go in each serp
    :return top n urls
    """

    urls = serp_df[serp_df['keyword'].isin(kws)]['urls'].values.tolist()
    distinct_urls = set([k for kl in urls for k in kl[0:n_serp_urls]])

    return distinct_urls


def get_url_overlap(serp_df, url_df, n_serp_urls=10):
    """
    Get set overlap between target url's serps and origin url's serps
    :param serp_df: df of serps for all keywords
    :param url_df: df of keywords for each url
    :param n_serp_urls: number of urls deep to go in each serp
    :return df of serp set overlaps
    """
    # get distinct urls for target (first row in df)
    distinct_target_urls = get_distinct_urls_in_serps(serp_df, list(url_df[url_df['origin'] == url_df['url']]['keywords'].values[0]), n_serp_urls=n_serp_urls)

    set_overlaps = []
    # compare to all other urls in the set
    for i in range(len(url_df)):
        distinct_urls = get_distinct_urls_in_serps(serp_df, url_df.loc[i, 'keywords'], n_serp_urls=n_serp_urls)

        try:
            set_overlap = round(len(distinct_target_urls.intersection(distinct_urls))/len(distinct_target_urls)*100)
            set_overlaps.append(set_overlap)

        except ZeroDivisionError:
            set_overlaps.append(np.nan)

    url_overlaps = pd.DataFrame(list(zip(url_df.url.values.tolist(), set_overlaps)), columns = ['url', 'url_set_overlap_with_target'])

    return url_overlaps


def build_doc_term_matrix(terms_lists, weighted=True):
    '''
    Construct a sparse document/term matrix, optionally weighted by the position of the terms in each document (i.e. in a SERP)
    :param terms_lists: list of urls
    :param weighted: weight by rank
    :return: sparse matrix of urls
    '''
    stringstore = StringStore()

    data = []
    rows = []
    cols = []
    for row_idx, terms_list in enumerate(terms_lists):
        bow = tuple((stringstore[term] - 1, 1. / (i ** 2 + 2) if weighted else 1) for i, term in enumerate(terms_list) if term)

        data.extend(count for _, count in bow)
        cols.extend(term_id for term_id, _ in bow)
        rows.extend(itertools.repeat(row_idx, times=len(bow)))

    #import pdb;pdb.set_trace()
    doc_term_matrix = sp.coo_matrix((data, (rows, cols)), dtype=float if weighted else int).tocsr()

    return doc_term_matrix


def get_weighted_url_sims(serp_df, url_df):
    """
    Gets serp similarity of origin urls to target url by constructing vectors for each urls keyword serps and measuring cosine similarity
    :param serp_df: df of serps for all keywords
    :param url_df: df of keywords for each url
    :return df of similarity of each origin url to the target url
    """

    keyword_index = serp_df.reset_index(drop=True).reset_index().set_index('keyword')['index']

    vol = serp_df.set_index('keyword')['volume_prediction'].fillna(0)

    urls = serp_df['urls'].values
    dtm = build_doc_term_matrix(urls.tolist(), weighted=True)

    url_vectors = []
    for i, row in url_df.iterrows():
        url = row['url']
        url_queries = row['keywords']
        qidx = keyword_index.loc[url_queries]
        dfc = pd.DataFrame(dtm[qidx, :].todense(), index=url_queries)
        dfc = (dfc.T * vol[url_queries]).T.sum(axis=0)
        dfc.name = url
        url_vectors.append(dfc)

    url_vectors = pd.concat(url_vectors, axis=1).T

    # NOTE: first url in url_vectors is the target, rest are the origin, we want similarity from target to origin
    sim_to_target = [round(x) for x in (1 - cdist(url_vectors, url_vectors, 'cosine')[0]) * 100]

    url_sims = pd.DataFrame(list(zip(url_vectors.index.tolist(), sim_to_target)), columns=['url', 'similarity_to_target'])
    return url_sims


def get_high_vol_keyword_overlap(url_df):
    '''
    get set overlap of the high volume keywords and create flag for whether the list has any keywords
    :param url_df: df of keywords for each url
    :return: dataframe with url, keyword overlap, and flag for has overlap
    '''
    origin_keys = list(url_df[url_df['origin'] == url_df['url']]['high_vol_rank_keywords'].values[0])
    overlaps = []
    flags = []
    for i, row in url_df.iterrows():
        if i == 0:
            overlaps.append(np.NaN)
            flags.append(np.NaN)
            continue
        overlap = list(set(row['high_vol_rank_keywords']).intersection(origin_keys))
        overlaps.append(overlap)
        if len(overlap) != 0:
            flags.append('Y')
        else:
            flags.append(np.NaN)

    key_overlap = pd.DataFrame([url_df['url'].values, overlaps, flags], index=['url', 'high_vol_rank_overlap', 'overlap_flag']).T
    return key_overlap


def get_text_similarity(url_df, dictionary, tfidf, sims, inds, sim_type):
    '''
    calculate the tfidf title similarity between origin and target
    :param url_df: df of keywords for each url
    :param dictionary: tfidf dictionary
    :param tfidf:
    :param sims: tfidf similarity matrix
    :param inds: index of order or titles
    :param sim_type: title or body
    :return: dataframe of urls and title similarity scores
    '''
    url_df[sim_type] = url_df[sim_type].fillna('')
    translator = str.maketrans('', '', string.punctuation)
    global_stemmer = PorterStemmer()

    # load all stopwords
    with open('/Users/thyde/Documents/cloned_proj_moat/project_moat/stopwords.txt') as f:
        stopwords = f.read().split()

    # parse text into list of word stems
    texts = [[global_stemmer.stem(word) for word in text.translate(translator).lower().split() if word not in stopwords]
             for text in url_df[sim_type].values]

    # calculate similarity score to target title
    sim_scores = []
    for text in texts:
        vec_bow = dictionary.doc2bow(text)
        vec_tfidf = tfidf[vec_bow]
        #import pdb;pdb.set_trace()
        res = sims[vec_tfidf]
        try:
            sim_scores.append(res[inds[url_df[url_df['origin'] == url_df['url']][sim_type].values[0]]])
        except KeyError:
            sim_scores.append(np.NaN)

    return pd.DataFrame([url_df['url'].values, sim_scores], index=['url', 'title_similarity']).T


def process_urls(urls, filename, es_index='recirc_loserlist',
                 n_widenet=100, n_wanted=5, boosts_wanted=['body.bigrams','body.unigrams','title.bigrams','title.unigrams'],
                 body_bigram_boost=4, body_unigram_boost=1, title_bigram_boost=4,
                 title_unigram_boost=1, min_should_match=1, min_doc_freq=0, min_term_freq=0, max_query_terms=50,
                 num_keywords=10, volume_cutoff=500, rank_cutoff=10, num_months_traffic=3, traffic_filter='gt 1000',
                 post_content_filter=[], n_serp_urls=10, title_dictionary=None, title_tfidf=None, title_sims=None, title_ind=None,
                 separate_subchannels=False, thresh_wanted=None, connection=None):
    '''
    get related content, keywords, serps, and traffic details for urls
    :param urls: list of urlObjs
    :param filename: output filename
    :param es_index: index name
    :param n_widenet: number of related articles to return
    :param n_wanted: number of related articles to output
    :param boosts_wanted: list of which title/body uni/bigram boosts to use
    :param body_bigram_boost:
    :param body_unigram_boost:
    :param title_bigram_boost:
    :param title_unigram_boost:
    :param min_should_match:
    :param min_doc_freq:
    :param min_term_freq:
    :param max_query_terms:
    :param num_keywords: the number of keywords to return or all
    :param volume_cutoff: minimum volume if high volume keywords wanted
    :param rank_cutoff: lowest rank if high rank keywords wanted
    :param num_months_traffic: number of months to average traffic over
    :param traffic_filter: filter for min or max page traffic in related articles
    :param post_content_filter: filter for content type in related articles
    :param n_serp_urls: number of urls to compare in serp url overlap
    :param title_dictionary: tfidf dictionary
    :param title_tfidf:
    :param title_sims: tfidf similarity matrix
    :param title_ind: index of title ordering
    :param separate_subchannels: flag to separate outputs by subchannel
    :param thresh_wanted: threshold of similarity score to return instead of n_wanted
    :return: outputs file of related content
    '''

    #get related content for urls
    related_content = batch_related_content(urls, es_index=es_index, n_widenet=n_widenet, boosts_wanted=boosts_wanted,
                                            body_bigram_boost=body_bigram_boost,
                                            body_unigram_boost=body_unigram_boost, title_bigram_boost=title_bigram_boost,
                                            title_unigram_boost=title_unigram_boost, min_should_match=min_should_match,
                                            min_doc_freq=min_doc_freq, min_term_freq=min_term_freq, max_query_terms=max_query_terms)  # throw a wide net to filter later

    related_content['url'] = related_content['url'].apply(lambda x: urlObj(x).presto)
    related_content['origin'] = related_content['origin'].apply(lambda x: urlObj(x).presto)

    # get top keywords for urls and related content
    print(' getting top keywords')
    if connection is None:
        connection = connect_presto()
    all_urls = [urlObj(url) for url in related_content['url'].values.tolist()]
    all_keywords = get_keywords_by_url(all_urls, connection=connection, num_keywords=num_keywords)
    all_keywords['url'] = all_keywords['url'].apply(lambda x: urlObj(x).presto)

    # get high volume and ranking keywords, if wanted, and merge with all keywords
    if volume_cutoff:
        print('getting high volume keywords')
        high_vol_rank_keywords = get_keywords_by_url(all_urls, connection=connection, num_keywords='all', volume_cutoff=volume_cutoff, rank_cutoff=rank_cutoff)
        high_vol_rank_keywords['url'] = high_vol_rank_keywords['url'].apply(lambda x: urlObj(x).presto)
        high_vol_rank_keywords.rename(columns={'keywords': 'high_vol_rank_keywords'}, inplace=True)
        url_df = all_keywords.groupby('url')['keywords'].apply(set).apply(list).reset_index().merge(
            high_vol_rank_keywords.groupby('url')['high_vol_rank_keywords'].apply(set).apply(list).reset_index(),
            on='url', how='outer')
    else:
        url_df = all_keywords.groupby('url')['keywords'].apply(set).apply(list).reset_index()

    # get serp details for all keywords
    print('getting serp details')
    serp_df = get_serp_by_keyword(all_keywords['keywords'].values.tolist(), connection=connection).rename(columns={'url': 'urls'})
    serp_df['urls'] = serp_df['urls'].apply(lambda d: d if isinstance(d, list) else [])

    # get page traffic
    #print('getting page traffic')
    #traffic_df = get_traffic_by_url([url.impala for url in all_urls]# )

    # merge related content, traffic, and serps and fill in blank lists
    #rc_traffic_keywords = pd.merge(related_content, traffic_df, how='left', on='url').merge(url_df, how='left', on='url')
    rc_traffic_keywords = related_content.merge(url_df, how='left', on='url')
    rc_traffic_keywords['keywords'] = rc_traffic_keywords['keywords'].apply(lambda d: d if isinstance(d, list) else [])
    #rc_traffic_keywords['high_vol_rank_keywords'] = rc_traffic_keywords['high_vol_rank_keywords'].apply(lambda d: d if isinstance(d, list) else [])

    # traffic filter
    if traffic_filter:
        traffic_filter = traffic_filter.split()
        if traffic_filter[0] == 'gt':
            filtered_df = rc_traffic_keywords[(rc_traffic_keywords['url'] == rc_traffic_keywords['origin']) | ((rc_traffic_keywords['content_type'] != 'Term') & (rc_traffic_keywords['avg_organic_entrances_last_3_months'] > int(traffic_filter[1])) | (rc_traffic_keywords['content_type'] == 'Term'))]
        elif traffic_filter[0] == 'lt':
            filtered_df = rc_traffic_keywords[(rc_traffic_keywords['url'] == rc_traffic_keywords['origin']) | (rc_traffic_keywords['avg_organic_entrances_last_3_months'] < int(traffic_filter[1]))]
        else:
            raise ValueError('Invalid Traffic Filter. Valid filters are "gt #" or "lt #"')
    else:
        filtered_df = rc_traffic_keywords

    # content type filter
    if post_content_filter:
        filtered_df = filtered_df[(~filtered_df['content_type'].isin(post_content_filter)) | (filtered_df['url'] == filtered_df['origin'])]

    print('gathering results')
    for url in frogress.bar(urls):
        # get top recirc urls to output
        if thresh_wanted and n_wanted:
            group = filtered_df[(filtered_df['origin'] == url.presto) & (filtered_df['text_sim_score'] > thresh_wanted)].nlargest(n_wanted+1, 'text_sim_score').reset_index().reset_index()
        elif thresh_wanted:
            group = filtered_df[(filtered_df['origin'] == url.presto) & (filtered_df['text_sim_score'] > thresh_wanted)].reset_index()
        else:
            group = filtered_df[filtered_df['origin'] == url.presto].nlargest(n_wanted+1, 'text_sim_score').reset_index()

        if group.empty:
            continue

        if url.presto not in group['url'].values:
            group = group.drop(group.index[len(group)-1]).append(filtered_df[(filtered_df['origin'] == url.presto) & (filtered_df['url'] == url.presto)]).reset_index(drop=True)


        # Compute similarity metrics
        url_overlaps = get_url_overlap(serp_df, group, n_serp_urls=n_serp_urls)
        #url_vector_sims = get_weighted_url_sims(serp_df, group)
        #high_vol_keyword_overlap = get_high_vol_keyword_overlap(group)
        title_similarity = get_text_similarity(group, title_dictionary, title_tfidf, title_sims, title_ind, 'title')

        # merge similarity metrics
        sim_df = pd.merge(group, url_overlaps, on='url').merge(title_similarity, on='url').drop('index', axis=1)
        sim_df['notes'] = sim_df.apply(lambda x: 'Main URL' if x['origin'] == x['url'] else 'Potential Dupe', axis=1)
        #sim_df['url_set_overlap_with_target'] = sim_df.apply(lambda x: 100 if pd.isnull(x[['notes'] == 'Main URL']) else x['url_set_overlap_with_target'], axis=1)
        sim_df=sim_df.fillna(0)
        sim_df['blended_score'] = ((10 / 17) * sim_df['text_sim_score'] + (6 / 17) * sim_df['title_similarity'] + (1 / 17) * (sim_df['url_set_overlap_with_target'] / 100))
        #sim_df['blended_score'] = ((10/14)*sim_df['text_sim_score']+(3/14)*sim_df['title_similarity']+(1/14)*(sim_df['url_set_overlap_with_target']/100))
        sim_df['blended_score'] = (sim_df['blended_score']*100).round(2)
        #import pdb;pdb.set_trace()

        #import pdb;pdb.set_trace()
        sim_df = sim_df.sort_values(['notes', 'blended_score'], ascending=[True, False])

        # round similarity scores
        sim_df['text_sim_score'] = sim_df['text_sim_score'].multiply(100).round(1)
        sim_df['title_similarity'] = sim_df['title_similarity'].astype(float).multiply(100).round(1)
        sim_df['avg_organic_entrances_last_3_months'] = sim_df['avg_organic_entrances_last_3_months'].round(0)
        sim_df[sim_df['notes'] == 'Main URL']['blended_score'] = 100
        # clean up dataframe with only desired columns
        cols = ['url', 'title', 'notes', 'avg_organic_entrances_last_3_months','blended_score','workflow_status']
        #import pdb;pdb.set_trace()
        clean_df = sim_df[cols]
        clean_df.rename(columns={'url':'URL','title':'Title','notes':'Potential Dupe/Main URL'
            ,'avg_organic_entrances_last_3_months':'Traffic','blended_score':'Blended Score','workflow_status':'Workflow Status'}, inplace=True)

        # separate by subchannel, if wanted, and write to file
        subchannel = sim_df[sim_df['notes'] == 'Main URL']['primary_sub_channel'].to_string(index=False)
        subchannel = format(subchannel).replace(' ', '_').replace('/', '')
        if separate_subchannels:
            refilename = filename+'_'+subchannel+'.csv'
        else:
            refilename=filename+'.csv'
        if os.path.isfile(refilename):
            clean_df.to_csv(refilename, mode = 'a', index = False, header = False)
        else:
            clean_df.to_csv(refilename, mode = 'a', index = False)

        # add blank lines
        with open(refilename, 'a') as f:
            f.write('\n\n')


def batch_process_urls(urls, filename, batchsize=10, es_index='recirc_loserlist',
                       n_widenet=100, n_wanted=5, boosts_wanted=['body.bigrams','body.unigrams','title.bigrams','title.unigrams'],
                       body_bigram_boost=4, body_unigram_boost=1, title_bigram_boost=4,
                       title_unigram_boost=1, min_should_match=1, min_doc_freq=0, min_term_freq=0, max_query_terms=50,
                       num_keywords=10, volume_cutoff=500, rank_cutoff=10, num_months_traffic=3,
                       traffic_filter='gt 1000', post_content_filter= [], n_serp_urls=10, title_dictionary=None,
                       title_tfidf=None, title_sims=None, title_ind=None, separate_subchannels=False, thresh_wanted=None):
    '''
    run the urls in batches
    :param urls: list of urlObjs
    :param filename: output filename
    :param es_index: index name
    :param n_widenet: number of related articles to return
    :param n_wanted: number of related articles to output
    :param boosts_wanted: list of which title/body uni/bigram boosts to use
    :param body_bigram_boost:
    :param body_unigram_boost:
    :param title_bigram_boost:
    :param title_unigram_boost:
    :param min_should_match:
    :param min_doc_freq:
    :param min_term_freq:
    :param max_query_terms:
    :param num_keywords: the number of keywords to return or all
    :param volume_cutoff: minimum volume if high volume keywords wanted
    :param rank_cutoff: lowest rank if high rank keywords wanted
    :param num_months_traffic: number of months to average traffic over
    :param traffic_filter: filter for min or max page traffic in related articles
    :param post_content_filter: filter for content type in related articles
    :param n_serp_urls: number of urls to compare in serp url overlap
    :param title_dictionary: tfidf dictionary
    :param title_tfidf:
    :param title_sims: tfidf similarity matrix
    :param title_ind: index of title ordering
    :param separate_subchannels: flag to separate outputs by subchannel
    :param thresh_wanted: threshold of similarity score to return instead of n_wanted
    :return: outputs file of related content
    '''

    def chunks(l, n):
        # For item i in a range that is a length of l,
        for i in range(0, len(l), n):
            # Create an index range for l of n items:
            yield l[i:i+n]

    urls = [urlObj(url) for url in urls]
    connection = connect_presto()
    for url_chunk in frogress.bar(list(chunks(urls, batchsize))):
        process_urls(url_chunk, filename=filename, boosts_wanted=boosts_wanted,
                     es_index=es_index, n_widenet=n_widenet, n_wanted=n_wanted, body_bigram_boost=body_bigram_boost,
                     body_unigram_boost=body_unigram_boost, title_bigram_boost=title_bigram_boost,
                     title_unigram_boost=title_unigram_boost, min_should_match=min_should_match,
                     min_doc_freq=min_doc_freq, min_term_freq=min_term_freq, max_query_terms=max_query_terms,
                     num_keywords=num_keywords, volume_cutoff=volume_cutoff, rank_cutoff=rank_cutoff,
                     num_months_traffic=num_months_traffic, traffic_filter=traffic_filter,
                     post_content_filter=post_content_filter, n_serp_urls=n_serp_urls,
                     title_dictionary=title_dictionary, title_tfidf=title_tfidf, title_sims=title_sims, title_ind=title_ind,
                     separate_subchannels=separate_subchannels, thresh_wanted=thresh_wanted, connection=connection)
