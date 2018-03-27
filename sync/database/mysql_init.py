# encoding: utf-8
import time, datetime
import pymysql as mydb
#import conf
from sync import conf

class mysql_handler:
	def __init__(self):
		self.user, self.password, self.db, self.host, self.port, self.type = conf.mysql_init()
		self.conn = None
		self.cursor = None
		try:
			self.conn = mydb.connect(host=self.host, user=self.user, passwd=self.password, db=self.db, port=self.port,cursorclass=mydb.cursors.DictCursor, charset='utf8')
			'''self.cursor = connect.cursor() tuple'''
			#self.cursor = self.conn.cursor(cursorclass=)
			self.cursor = self.conn.cursor()
			#self.cursor.execute('SET NAMES utf8mb4')
			#self.cursor.execute("SET CHARACTER SET utf8mb4")
			#self.cursor.execute("SET character_set_connection=utf8mb4")
			#self.conn.commit()
		except mydb.Error as e:
			print("Mysql Error %d: %s" % (e.args[0], e.args[1]))
	'''update mysql database webpage table count as mysql service status '''
	def update(self):
		sql = "SELECT COUNT(*) FROM webpage"
		count = 0
		try:
			self.cursor.execute(sql)
			results = self.cursor.fetchall()
			count = results[0]['COUNT(*)']
		except mydb.Error as e:
			print e
		mysql_time = time.strftime("%Y-%m-%d %H:%M:%S")
		mysql_list = [self.host, mysql_time, count, self.type]
		return mysql_list
	def __update_same(self, sql):
		try:
			self.cursor.execute(sql)
			self.conn.commit()
			print 'update_successful: %s' % sql
		except mydb.Error as e:
			print
	def update_node(self, node_ip, node_time, node_state, node_type):
		sql = "UPDATE node SET node_time='%s', node_state=%d where node_ip='%s' and node_type=%d" % (node_time, node_state, node_ip, node_type)
		try:
			self.cursor.execute(sql)
			self.conn.commit()
			print 'update node:%s type:%d status successful' % (node_ip, node_type)
		except mydb.Error as e:
			print e
	def update_site(self, count, site_id):
		mysql_time = time.strftime("%Y-%m-%d %H:%M:%S")
		sql = "update site set site_count=%d ,site_last_time='%s' where id=%d" % (count, mysql_time, site_id)
		self.__update_same(sql)
	def select_topic_list(self):
		sql = 'SELECT t.id as topic_id,t.topic_in_words as in_words,t.topic_ex_words as ex_words,topic_name as topic_name ,u.id as user_id FROM topic t,user u where t.user_id=u.id and u.is_active=1 and t.status=2'
		return self.__select_list(sql)
	def select_site_list(self):
		sql = 'SELECT id as site_id,site_name from site'
		return self.__select_list(sql)
	def select_synonymdict_list(self,syno_word):
		sql = "SELECT syno_name,syno_rela FROM synonymdict where syno_name='%s'" % syno_word
		return self.__select_list(sql)
	def select_content(self):
		sql = 'SELECT content,title,id,url from webpage where id>11455'
		return self.__select_list(sql)
	def update_webpage_summ(self, topic_word, id):
		sql = "update webpage set summ='%s' where id=%d" % (topic_word, id)
		self.__update_same(sql)
	def update_webpage_sensibilities(self, sensibilities, id):
		sql = "update webpage set sensibilities=%d where id=%d" % (sensibilities, id)
		self.__update_same(sql)
	def update_webpage_section_name(self, id, section_name):
		sql = "update webpage set section_name='%s' where id=%d" % (section_name, id)
		self.__update_same(sql)
	def select_webpage_id(self, url):
		sql = "SELECT id,pub_date FROM webpage where url='%s'" % url
		list_id_pub_date = self.__select_list(sql)
		if len(list_id_pub_date) > 0 :
			id_int = list_id_pub_date[0]['id']
			pub_date = list_id_pub_date[0]['pub_date']
			return id_int, pub_date
		else:
			return None, None
	def __select_list(self, sql):
		list = []
		try:
			self.cursor.execute(sql)
			results = self.cursor.fetchall()
			for row in results:
				list.append(row)
			return list
		except mydb.Error as e:
			print e
		return []
	def __insert_sql(self, sql):
		try:
			self.cursor.execute(sql)
			self.conn.commit()
			return True
		except mydb.Error as e:
			print 'insert error:'+sql
			print e
			self.conn.rollback()
			return False
	def insert_webpage(self, dict_webpage):
		sql = '''insert into webpage (title, summ, url, pub_date, crawl_date, site_name, is_read, is_collect, is_push, sensibilities, content, author, section_name, site_type)
			  '''+ " values('%s','%s','%s','%s','%s','%s',%d, %d, %d, %d,'%s','%s', '%s', %d)" % (dict_webpage['title'], dict_webpage['summ'], dict_webpage['url'], str(dict_webpage['public_time']),str(dict_webpage['crawl_time']), dict_webpage['site_name'], 0, 0, 0, dict_webpage['sensibilities'], dict_webpage['content'], dict_webpage['author'], dict_webpage['section_name'], dict_webpage['site_type'])
		self.__insert_sql(sql)
	def insert_re_topic(self, web_id, topic_id, pub_date, user_id):
		sql = 'insert into re_web_topic (web_id, topic_id, crawl_date, user_id) values(%d, %d, "%s", %d)' % (web_id, topic_id, pub_date, user_id)
		#table crawl_date -> pub_date
		#print sql
		self.__insert_sql(sql)
	def __is_exist(self, sql):
		self.cursor.execute(sql)
		results = self.cursor.fetchall()
		count = len(results)
		if (count >= 1):
			return True
		else:
			return False
	def is_webpage(self, web_url):
		sql = "SELECT * FROM webpage where url='%s'" % web_url
		return self.__is_exist(sql)
	def is_re_webtopic(self, web_id, topic_id, user_id):
		sql = "SELECT * FROM re_web_topic where web_id=%d and topic_id=%d and user_id=%d " % (web_id, topic_id, user_id)
		return self.__is_exist(sql)
	def close(self):
		self.cursor.close()
		self.conn.close()
