# encoding: utf-8
import redis
import time
import conf
class redis_handler:
	def __init__(self):
		self.host, self.port, self.db, self.password, self.url_list, self.type = conf.redis_init()
		try:
			pool = redis.ConnectionPool(host=self.host, port=self.port, db=self.db, password=self.password)
			#self.r = redis.StrictRedis(host=self.host, port=self.port, db=self.db, password=self.password)
			self.r = redis.Redis(connection_pool=pool)
		except redis.Error as e:
			print("Redis Error %d: %s" % (e.args[0], e.args[1]))
	def update(self):
		url_list = []
		for i in range(len(self.url_list)):
			status = self.r.get(self.url_list[i][0])
			node_time = status[5:24]
			node_count = int(status[27:-1])
			clawer_list = [self.url_list[i][0], node_time, node_count, self.url_list[i][1]]
			url_list.append(clawer_list)
		redis_count = self.r.scard('isearchSpider:dupefilter')
		redis_time = time.strftime("%Y-%m-%d %H:%M:%S")
		list = [self.host, redis_time, redis_count, self.type]
		url_list.append(list)
		return url_list

