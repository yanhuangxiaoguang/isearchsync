# encoding: utf-8
import time
from pymongo import MongoClient
import conf
class mongo_handler:
	def __init__(self):
		self.host, self.port, self.user, self.password, self.database, self.type = conf.mongo_init()
		try:
			self.client = MongoClient(self.host, self.port)
			db_auth = self.client.admin
			db_auth.authenticate(self.user, self.password)
			self.db = self.client[self.database]
		except MongoClient.Error as e:
			print("Mongo Error %d: %s" % (e.args[0], e.args[1]))
	def update(self):
		collection_name = time.strftime("%Y-%m-%d")
		count = self.db[collection_name].count()
		mongo_time = time.strftime("%Y-%m-%d %H:%M:%S")
		mongo_list = [self.host, mongo_time, count, self.type]
		return mongo_list
	def close(self):
		self.client.close()
