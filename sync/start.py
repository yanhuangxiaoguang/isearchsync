# encoding: utf-8
import time
from time import sleep
from database.mysql_init import mysql_handler
from database.redis_init import redis_handler
from database.mongo_init import mongo_handler
from database.esrtf_init import esrtf_handler
from database.analysis.key_word import top_word
from database.analysis import analyize
mysql_conn = mysql_handler()
redis_conn = redis_handler()
mongo_conn = mongo_handler()
esrtf_conn = esrtf_handler()
def update_service():
	'''3.28微博框架'''
	'''3.28微博框sdasd架'''
	'update redis and clawer status'
	redis_list = redis_conn.update()
	for i in range(len(redis_list)):
		mysql_conn.update_node(redis_list[i][0], redis_list[i][1], redis_list[i][2], redis_list[i][3])
	'update mysql status'
	mysql_dict = mysql_conn.update()
	mysql_conn.update_node(mysql_dict[0], mysql_dict[1], mysql_dict[2], mysql_dict[3])
	'update mongo status'
	mongo_list = mongo_conn.update()
	mysql_conn.update_node(mongo_list[0], mongo_list[1], mongo_list[2], mongo_list[3])
	'update elasticsearch-rtf status'
	esrtf_list = esrtf_conn.update()
	mysql_conn.update_node(esrtf_list[0], esrtf_list[1], esrtf_list[2], esrtf_list[3])
	'update site status'
	site_list = mysql_conn.select_site_list()
	for i in range(len(site_list)):
		#print site_list[i]['site_name']
		count = esrtf_conn.updete_site(site_list[i]['site_name'])
		#print count
		mysql_conn.update_site(count, site_list[i]['site_id'])
def is_a_week(time1, time2):
	public_int = time.strptime(time1, "%Y-%m-%d %H:%M:%S")
	public_stamp = int(time.mktime(public_int))
	crawl_int = time.strptime(time2, "%Y-%m-%d %H:%M:%S")
	crawl_stamp = int(time.mktime(crawl_int))
	#print public_stamp
	grup = (crawl_stamp - public_stamp) / 3600
	if grup > 168:
		print "Publication time is more than one week of collection time, data is not pushed"
		return False
	else:
		return True
	
def handle_response(responses,topic_id, user_id):
	for hit in responses:
		if mysql_conn.is_webpage(hit['url']):
			web_id, web_pub_time = mysql_conn.select_webpage_id(url=hit['url'])
			if mysql_conn.is_re_webtopic(web_id=web_id, topic_id=topic_id, user_id=user_id):
				# print "The data is exist，web_id: %d " % web_id
				pass
			else:
				# print 'insert new data  to webpage'
				mysql_conn.insert_re_topic(web_id=web_id, topic_id=topic_id, pub_date=web_pub_time, user_id=user_id)
		else:
			webpage_dict = {}
			webpage_dict['url'] = hit['url']
			webpage_dict['title'] = hit['title']
			webpage_dict['author'] = hit['author']
			webpage_dict['content'] = hit['content']
			webpage_dict['site_name'] = hit['site_name']
			webpage_dict['site_type'] = hit['site_type']
			webpage_dict['crawl_time'] = hit['crawl_time']
			webpage_dict['public_time'] = hit['public_time']
			webpage_dict['section_name'] = hit['section_name']
			if not hit['public_time']:
				webpage_dict['public_time'] = webpage_dict['crawl_time']
			elif is_a_week(hit['public_time'], hit['crawl_time']):
				# print 'Published within a week'
				pass
			else:
				print 'Published over than a week'
				continue
			webpage_dict['sensibilities'] = analyize.emotion(hit['title'], hit['content'])
			webpage_dict['summ'] = top_word(hit['content'])
			mysql_conn.insert_webpage(dict_webpage=webpage_dict)
			web_id, pub_date_tmp = mysql_conn.select_webpage_id(url=hit['url'])
			if web_id:
				mysql_conn.insert_re_topic(web_id=web_id, topic_id=topic_id, pub_date=pub_date_tmp, user_id=user_id)
			else:
				print 'do not have found webpage id, maybe the date Maybe the data is being inserted into the MySQL database, and the database is not refreshed'

def handle_response_weibo(responses,topic_id, user_id):
	for hit in responses:
		if mysql_conn.is_webpage(hit['url']): #Id
			web_id, web_pub_time = mysql_conn.select_webpage_id(url=hit['url'])
			if mysql_conn.is_re_webtopic(web_id=web_id, topic_id=topic_id, user_id=user_id):
				# print "The data is exist，web_id: %d " % web_id
				pass
			else:
				# print 'insert new data  to webpage'
				mysql_conn.insert_re_topic(web_id=web_id, topic_id=topic_id, pub_date=web_pub_time, user_id=user_id)
			'''if data_update :
				update weibo weibo_comment weibo_tranfer
			else:
				pass
			'''
		else:
			webpage_dict = {}
			webpage_dict['url'] = hit['url']
			webpage_dict['title'] = hit['title']
			webpage_dict['author'] = hit['author']
			webpage_dict['content'] = hit['content']
			webpage_dict['site_name'] = hit['site_name']
			webpage_dict['site_type'] = hit['site_type']
			webpage_dict['crawl_time'] = hit['crawl_time']
			webpage_dict['public_time'] = hit['public_time']
			webpage_dict['section_name'] = hit['section_name']
			if not hit['public_time']:
				webpage_dict['public_time'] = webpage_dict['crawl_time']
			elif is_a_week(hit['public_time'], hit['crawl_time']):
				# print 'Published within a week'
				pass
			else:
				print 'Published over than a week'
				continue
			webpage_dict['sensibilities'] = analyize.emotion(hit['title'], hit['content'])
			webpage_dict['summ'] = top_word(hit['content'])
			mysql_conn.insert_webpage(dict_webpage=webpage_dict)
			'''insert weibo_comment weibo_transfer'''
			web_id, pub_date_tmp = mysql_conn.select_webpage_id(url=hit['url'])
			if web_id:
				mysql_conn.insert_re_topic(web_id=web_id, topic_id=topic_id, pub_date=pub_date_tmp, user_id=user_id)
			else:
				print 'do not have found webpage id, maybe the date Maybe the data is being inserted into the MySQL database, and the database is not refreshed'

def sync_es_info():
	topic_list = mysql_conn.select_topic_list()
	for i in range(len(topic_list)):
		topic_id = topic_list[i]['topic_id']
		user_id = topic_list[i]['user_id']
		str_in_words = topic_list[i]['in_words']
		str_ex_words = topic_list[i]['ex_words']
		response = esrtf_conn.search_syno(mysql_conn, str_in_words, str_ex_words, key=1)
		try:
			handle_response(responses=response, topic_id=topic_id, user_id=user_id)
		except:
			print "handle error: in_words:%s, sex_words %s " % (str_in_words,str_ex_words)
			
		weibo_response = esrtf_conn.search_syno(mysql_conn, str_in_words, str_ex_words, key=2, list_fields=["Content"])
		try:
			return
			handle_response_weibo(weibo_response,topic_id=topic_id, user_id=user_id)
		except:
			print "handle error: in_words:%s, sex_words %s " % (str_in_words, str_ex_words)
		return

def close_all():
	mongo_conn.close()
	mysql_conn.close()
	
p = open("signal.txt", 'w')
p.write('start')
p.close()
print "syno start"
sync_es_info()
sleep(2)
update_service()
close_all()
print "syno stop"
f = open("signal.txt", 'w')
f.write('over')
f.close()
sleep(2)	

