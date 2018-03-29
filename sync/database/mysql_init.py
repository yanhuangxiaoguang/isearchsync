# encoding: utf-8
import time, datetime
import pymysql as mydb
import conf
class mysql_handler:
	def __init__(self):
		self.user, self.password, self.db, self.host, self.port, self.type = conf.mysql_init()
		self.conn = None
		self.cursor = None
		try:
			self.conn = mydb.connect(host=self.host, user=self.user, passwd=self.password, db=self.db, port=self.port, charset='utf8', cursorclass=mydb.cursors.DictCursor)
			'''self.cursor = connect.cursor() tuple'''
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
			#print 'update_successful: %s' % sql
		except mydb.Error as e:
			print
	def update_node(self, node_ip, node_time, node_state, node_type):
		sql = "UPDATE node SET node_time='%s', node_state=%d where node_ip='%s' and node_type=%d" % (node_time, node_state, node_ip, node_type)
		try:
			self.cursor.execute(sql)
			self.conn.commit()
			print '%s update node:%s status successful' % (time.strftime("%Y-%m-%d %H:%M:%S"), node_ip)
		except mydb.Error as e:
			print e
	def update_site(self, count, site_id):
		mysql_time = time.strftime("%Y-%m-%d %H:%M:%S")
		sql = "update site set site_count=%d ,site_last_time='%s' where id=%d" % (count, mysql_time, site_id)
		self.__update_same(sql)
	def update_webpage_summ(self, topic_word, id):
		sql = "update webpage set summ='%s' where id=%d" % (topic_word, id)
		self.__update_same(sql)
	def update_webpage_sensibilities(self, sensibilities, id):
		sql = "update webpage set sensibilities=%d where id=%d" % (sensibilities, id)
		self.__update_same(sql)
	def update_webpage_section_name(self, id, section_name):
		sql = "update webpage set section_name='%s' where id=%d" % (section_name, id)
		self.__update_same(sql)
	def update_weibo(self, weibo_id, like_num, transfer_num, comment_num):
		sql = "update weibo set like_num=%d, transger_num=%d, comment_num=%d where weibo_id=%d " % ( like_num, transfer_num, comment_num, weibo_id)
		#print sql
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
  	def select_webpage_id(self, url):
		sql = "SELECT id,pub_date FROM webpage where url='%s'" % url
		list_id_pub_date = self.__select_list(sql)
		if len(list_id_pub_date) > 0 :
			id_int = list_id_pub_date[0]['id']
			pub_date = list_id_pub_date[0]['pub_date']
			return id_int, pub_date
		else:
			return None, None
		
	def seleet_weibo_comment_num(self, weibo_id):
		sql = "select count(*) from weibo_comment where comment_id='%s'" % weibo_id
		self.__select_count(sql)
	
	def seleet_weibo_transfer_num(self, weibo_id):
		sql = "select count(*) from weibo_transfer where transfer_id='%s'" % weibo_id
		self.__select_count(sql)
	
	def __select_count(self,sql):
		try:
			self.cursor.execute(sql)
			results = self.cursor.fetchall()
			count = results[0]['count(*)']
		except mydb.Error as e:
			print e
			count = 0
		return count
	
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
			#print 'insert error:'+sql
			print e
			self.conn.rollback()
			return False
	def insert_webpage(self, dict_webpage):
		sql = '''insert into webpage (title, summ, url, pub_date, crawl_date, site_name, is_read, is_collect, is_push, sensibilities, content, author, section_name, site_type)
			  '''+ " values('%s','%s','%s','%s','%s','%s',%d, %d, %d, %d,'%s','%s', '%s', %d)" % (dict_webpage['title'], dict_webpage['summ'], dict_webpage['url'], str(dict_webpage['public_time']),str(dict_webpage['crawl_time']), dict_webpage['site_name'], 0, 0, 0, dict_webpage['sensibilities'], dict_webpage['content'], dict_webpage['author'], dict_webpage['section_name'], dict_webpage['site_type'])
		#print sql
		self.__insert_sql(sql)
	def insert_re_topic(self, web_id, topic_id, pub_date, user_id):
		sql = 'insert into re_web_topic (web_id, topic_id, crawl_date, user_id) values(%d, %d, "%s", %d)' % (web_id, topic_id, pub_date, user_id)
		self.__insert_sql(sql)
	def insert_weibo_user(self, dict_weibo_users):
		for dict_user in dict_weibo_users:
			if not dict_user.has_key('BriefIntroduction'):
				dict_user['BriefIntroduction'] = 'NO'
			sql = "insert into weibo_user (id, nickname, location, brief_intr, auth, url, viplevel, fans, follows, tweets) values('%s', '%s', '%s', '%s', '%s', '%s', %d, '%s', '%s', '%s');" % (dict_user['Id'], dict_user['NickName'], dict_user['Location'], dict_user['BriefIntroduction'], dict_user['Authentication'], dict_user['Url'], int(dict_user['Viplevel']), dict_user['Fans'], dict_user['Follows'], dict_user['Tweets'])
			self.__insert_sql(sql)
	def insert_weibo(self, dict_weibo):
		sql = "insert into weibo (weibo_id, user_id, nickname, content, pubtime, tools, like_num, transfer_num, comment_num, cooridinates) values('%s', '%s', '%s', '%s', '%s', '%s', %d, %d, %d, '%s')" % (dict_weibo['id'], dict_weibo['Id'], dict_weibo['NickName'], dict_weibo['Content'], dict_weibo['PubTime'], dict_weibo['Tools'], dict_weibo['Like'], dict_weibo['Transfer'], dict_weibo['Comment'], dict_weibo['Co_oridinates'])
		#print sql
		self.__insert_sql(sql)
	def insert_weibo_comment(self, weibo_id, dict_weibo_comment):
		if self.has_weibo_comment(weibo_id, dict_weibo_comment):
			sql = "update weibo_comment set like_num=%d where comment_id='%s' and uid='%s' and comment_str='%s'" % (dict_weibo_comment['like'], weibo_id, dict_weibo_comment['uid'], dict_weibo_comment['comment'])
			#print sql
			self.__update_same(sql)
		else:
			sql = "insert into weibo_comment (comment_id, name, uid, comment_str, like_num) values ('%s', '%s' , '%s', '%s', %d)" % (weibo_id, dict_weibo_comment['name'], dict_weibo_comment['uid'], dict_weibo_comment['comment'], dict_weibo_comment['like'])
			#print sql
			self.__insert_sql(sql)
	def insert_weibo_transfer(self, weibo_id, dict_weibo_transfer):
		if self.has_weibo_transfer(weibo_id, dict_weibo_transfer):
			sql =  "update weibo_transfer set like_num=%d where transfer_id='%s' and uid='%s' and content='%s'" % (dict_weibo_transfer['like'], weibo_id, dict_weibo_transfer['uid'], dict_weibo_transfer['content'])
			#print sql
			self.__update_same(sql)
		else:
			sql = "insert into weibo_transfer (transfer_id, name, uid, content, like_num) values ('%s', '%s' , '%s', '%s', %d)" % (weibo_id, dict_weibo_transfer['name'], dict_weibo_transfer['uid'], dict_weibo_transfer['content'], dict_weibo_transfer['like'])
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
	def has_weibo(self, weibo_id):
		sql = "select * from weibo where weibo_id='%s'" % weibo_id
		#print sql
		return self.__is_exist(sql)
	def has_weibo_transfer(self, weibo_id, dict_weibo_transfer):
		sql = "select * from weibo_transfer where transfer_id='%s' and uid='%s' and content='%s'" % (weibo_id, dict_weibo_transfer['uid'], dict_weibo_transfer['content'])
		return self.__is_exist(sql)
	def has_weibo_comment(self, weibo_id, dict_weibo_comment):
		sql = "select * from weibo_comment where comment_id='%s' and uid='%s' and comment_str='%s' " % (weibo_id, dict_weibo_comment['uid'], dict_weibo_comment['comment'])
		return self.__is_exist(sql)
	def is_re_webtopic(self, web_id, topic_id, user_id):
		sql = "SELECT * FROM re_web_topic where web_id=%d and topic_id=%d and user_id=%d " % (web_id, topic_id, user_id)
		return self.__is_exist(sql)
	def close(self):
		self.cursor.close()
		self.conn.close()
		
# mysql_conn = mysql_handler()
# mysql_conn.seleet_weibo_comment_num('5183194482-M_FEW5lwtJw')