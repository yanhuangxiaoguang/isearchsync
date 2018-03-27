# encoding: utf-8
from database.mysql_init import mysql_handler
from database.mongo_init import mongo_handler

mysql_conn = mysql_handler()
mongo_conn = mongo_handler()

def import_weibo_user():
    user_dict = mongo_conn.selset_weibo_user()
    '''将user从Mongodb导入到mysql'''
    #mysql_conn.insert_weibo_user(user_dict)
    
    mongo_conn.close()
    mysql_conn.close()
import_weibo_user()