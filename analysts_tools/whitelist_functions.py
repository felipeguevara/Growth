import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from datetime import date, datetime, timedelta 
import re
import json    
import time
import sys
sys.path.append('../')
from analysts_tools.growth import *
from analysts_tools.frucap import *
from analysts_tools.Clusters_querys import *
from analysts_tools.whitelist_functions import *
from procurement_lib import send_slack_notification
import pandas as pd
import numpy as np
import sys
from procurement_lib import DataWarehouse, GoogleSheet
from analystcommunity import frubana_logger as logging
import logging as pylogging
from datetime import datetime, timedelta
import time
import warnings
import pytz
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk import ngrams
from nltk.stem import WordNetLemmatizer
from nltk import wordnet
from fuzzywuzzy import process
from fuzzywuzzy import fuzz
import nltk
import re
from collections import Counter
dw = DataWarehouse()
################################################################################################################################################################################################################
################################################################################################################################################################################################################


def run_customQuery(query):
    """
    Esta función corre el query que entre como parametro en el data warehouse
    
    Argumentos:
    query: el query para correr en el DW
    
    Resultados:
    data_discount: dataframe con los order_items_id con descuento.
    """
    dataframe = read_connection_data_warehouse.runQuery(query)
    
    return dataframe

################################################################################################################################################################################################################
################################################################################################################################################################################################################

def solicitar_query_as_df(api_key, query_id):
    """
    Esta función corre un query creado en redash
    
    Argumentos:
    api_key: api key (propia de cada usuario en redash)
    query_id: (id del query en redash - sale en la URL)
    
    Resultados:
    data_discount: dataframe con los order_items_id con descuento.
    """
    import http.client
    import json
    import time
    import pandas as pd
    print('connecting to redash...')
    h1 = http.client.HTTPSConnection('redash.federate.frubana.com')
    h1.request('POST', '/api/queries/{query_id}/results'.format(query_id=query_id), body=json.dumps({'max_age': 0}), headers={'Authorization':api_key})
    jobs_response = h1.getresponse()
    jobs_bytes = jobs_response.read()
    status = json.loads(jobs_bytes)['job']['status']
    id_jobs = json.loads(jobs_bytes)['job']['id']
    time.sleep(5)
    while status in [1, 2]:
        h1.request('GET', f'/api/jobs/{id_jobs}', body=None, headers={'Authorization':api_key})
        jobs_in_progess = h1.getresponse()
        jobs_bytes_in_progess = jobs_in_progess.read()
        status = json.loads(jobs_bytes_in_progess)['job']['status']
        print('query_executing...')
        time.sleep(20)
    print('importing data...')
    h1.request('GET', '/api/queries/{query_id}/results'.format(query_id=query_id), body=None, headers={'Authorization':api_key})
    query_response = h1.getresponse()
    print('reading data...')
    a = query_response.read()
    df = pd.DataFrame(json.loads(a)['query_result']['data']['rows'])
    print('Done')
    

################################################################################################################################################################################################################
################################################################################################################################################################################################################

def query_check_segmentos():
    """
    Esta función extrae la data de la primera orden
    
    """
    query = """
    select distinct bc.customer_id,
    business_type_id,
    segmentation_stage,
    country_code,
    first_commercial,
    bc.first_name as user_name

    from postgres_broadleaf_federate."broadleaf.blc_customer" bc
    inner join postgres_broadleaf_federate."broadleaf.fb_customer" fb on bc.customer_id=fb.customer_id
    inner join postgres_broadleaf_federate."broadleaf.blc_order" bo on bc.customer_id=bo.customer_id

    """
    
    dataframe = read_connection_data_warehouse.runQuery(query)
    
    return dataframe

################################################################################################################################################################################################################
################################################################################################################################################################################################################

def MS_corregidos(base,business_type_restaurante):
    """
    Esta función filtro a los clientes MS correcto
    
    """
    Choices_MS_id_BAQ=[1,3,4,5,11,10,9,2,12]
    Choices_MS_id_BOG=[1,3,4,5,6,7,8,2]
    Choices_MS_id_CMX=[1,13,14,15,16,17,6,7,2]
    Choices_MS_id_SPO=[19,18,20,21,22,23,24,26,25]
    
    filtro_bog=(base['microsegment_id'].isin(Choices_MS_id_BOG)) & (base['city']=='BOG')
    filtro_cmx=(base['microsegment_id'].isin(Choices_MS_id_CMX)) & (base['city']=='CMX')
    filtro_spo=(base['microsegment_id'].isin(Choices_MS_id_SPO)) & (base['city']=='SPO')
    filtro_baq=(base['microsegment_id'].isin(Choices_MS_id_BAQ)) & (base['city']=='BAQ')

    filtro_final= (filtro_bog) | (filtro_cmx) | (filtro_spo) | (filtro_baq)
    
    dataframe_1 = base[filtro_final]
    if business_type_restaurante=='Si':
        dataframe   =dataframe_1[dataframe_1['business_type_id']==1]
    else:
        dataframe=dataframe_1
    return dataframe

################################################################################################################################################################################################################
################################################################################################################################################################################################################

def clean_words(tokens):
    
    """
    Owner: Gabo Moreno 
    Esta función limpia los tokens
    
    """
    
    tokens=[t.lower() for t in tokens]

    tokens = [re.sub(r"[àáâãäå]", 'a', t ) for t in tokens]
    tokens = [re.sub(r"[èéêë]", 'e', t ) for t in tokens]
    tokens = [re.sub(r"[ìíîï]", 'i', t ) for t in tokens]
    tokens = [re.sub(r"[òóôõö]", 'o', t ) for t in tokens]
    tokens = [re.sub(r"[ùúûü]", 'u', t ) for t in tokens]
    tokens = [re.sub(r"[ýÿ]", 'y', t ) for t in tokens]
    tokens = [re.sub(r"[ß]", 'ss', t ) for t in tokens]
    tokens = [re.sub(r"[ñ]", 'n', t ) for t in tokens]
    
    tokens=[t for t in tokens if t not in stopwords.words('english')]
    tokens=[t for t in tokens if t not in stopwords.words('spanish')]
    tokens=[t for t in tokens if t not in stopwords.words('portuguese')]

    tokens=[ t for t in tokens if t.isalpha()]
    lemmatizer=WordNetLemmatizer()
    tokens=[lemmatizer.lemmatize(t) for t in tokens]
    
    return tokens


################################################################################################################################################################################################################
################################################################################################################################################################################################################

def counter_words(df,ngrama):
    
    """
    Esta función realiza el conteo de palabras
    
    df: base de datos
    ngrama: números de ngramas para realizar el conteo
    
    """
    
    big_string=" ".join(df['user_name'])
    tokenize=word_tokenize(big_string)
    clean_user_name=clean_words(tokenize)
    if ngrama ==1:
        counter_productos=Counter(clean_user_name)
        DataFrame=pd.DataFrame(counter_productos.items(),columns=['word','frequency']).sort_values(by='frequency',ascending=False)
    else:
        counter_productos=Counter(ngrams(clean_user_name,ngrama))
        DataFrame=pd.DataFrame(counter_productos.items(),columns=['word','frequency']).sort_values(by='frequency',ascending=False)
    return DataFrame


def MS_corregidos_2(base):
    """
    Esta función filtro a los clientes MS correcto
    
    """
    Choices_MS_id_BAQ=[1,3,4,5,11,10,9,2,12]
    Choices_MS_id_BOG=[1,3,4,5,6,7,8,2]
    Choices_MS_id_CMX=[1,13,14,15,16,17,6,7,2]
    Choices_MS_id_SPO=[19,18,20,21,22,23,24,26,25]
    
    filtro_bog=(base['microsegment_id'].isin(Choices_MS_id_BOG)) & (base['region_code']=='BOG')
    filtro_cmx=(base['microsegment_id'].isin(Choices_MS_id_CMX)) & (base['region_code']=='CMX')
    filtro_spo=(base['microsegment_id'].isin(Choices_MS_id_SPO)) & (base['region_code']=='SPO')
    filtro_baq=(base['microsegment_id'].isin(Choices_MS_id_BAQ)) & (base['region_code']=='BAQ')

    filtro_final= (filtro_bog) | (filtro_cmx) | (filtro_spo) | (filtro_baq)
    
    dataframe = base[filtro_final]
    return dataframe

################################################################################################################################################################################################################
################################################################################################################################################################################################################

def clean_words_only_string(tokens):
    
    """
    Esta función limpia los tokens
    
    """
    
    tokens=tokens.lower() 

    tokens = re.sub(r"[àáâãäå]", 'a', tokens ) 
    tokens = re.sub(r"[èéêë]", 'e', tokens ) 
    tokens = re.sub(r"[ìíîï]", 'i', tokens )
    tokens = re.sub(r"[òóôõö]", 'o', tokens )
    tokens = re.sub(r"[ùúûü]", 'u', tokens ) 
    tokens = re.sub(r"[ýÿ]", 'y', tokens ) 
    tokens = re.sub(r"[ß]", 'ss', tokens )
    tokens = re.sub(r"[ñ]", 'n', tokens ) 
    
    return tokens

################################################################################################################################################################################################################
################################################################################################################################################################################################################

def clean_words_only_stop_words(tokens):
    
    """
    Esta función limpia los tokens
    
    """
    

    tokens=[t for t in tokens if t not in stopwords.words('english')]
    tokens=[t for t in tokens if t not in stopwords.words('spanish')]
    tokens=[t for t in tokens if t not in stopwords.words('portuguese')]
    tokens=[ t for t in tokens if t.isalpha()]

    lemmatizer=WordNetLemmatizer()
    tokens=[lemmatizer.lemmatize(t) for t in tokens]
    
    return tokens

################################################################################################################################################################################################################
################################################################################################################################################################################################################
