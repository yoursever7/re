# !usr/bin/python
# Filename: config.py

USERS_PATH           = '/data/recommend/offline/tmp_data/user.data'
ITEMS_PATH           = '/data/recommend/offline/tmp_data/item.data'
CF_RESULT_PATH       = '/data/recommend/offline/tmp_data/global_cf_result.data'
CLASSIFY_RESULT_PATH = '/data/recommend/offline/tmp_data/global_classify_result.data'
MODEL_PATH           = '/data/recommend/offline/tmp_data/alsmodel'

RATINGS_BASE_PATH    = '/data/recommend/offline/tmp_data/rating.base'
RATINGS_ALL_PATH     = '/data/recommend/offline/tmp_data/rating.all'
PARAMS_PATH          = '/data/recommend/offline/config/alsmodel.config'

MYSQL_HOST = '10.105.50.42'
MYSQL_USER = 'analysis'
MYSQL_PWD  = 'qinglong123'
MYSQL_DB   = 'interface'
MYSQL_PORT = 3306

REDIS_HOST = '10.105.247.189'
REDIS_PWD  = 'recommendredis'
REDIS_PORT = 6379

NEW_START_DAY = 1000
HOT_START_DAY = 10
SAMPLE_START_DAY = 1000

RECOMMEND_NUM = 20
ALS_RECOMMEND_K = 20
LIMIT_ALS_RECOMMEND_LEN = 5
CLASSIFY_RECOMMEND_K = 20
