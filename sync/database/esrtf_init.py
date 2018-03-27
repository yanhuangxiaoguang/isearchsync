# encoding: utf-8
import time
import conf
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, analyzer
from elasticsearch_dsl.query import Q

from mysql_init import mysql_handler
#doc_type = time.strftime("%Y-%m-%d")

class esrtf_handler:
	def __init__(self):
		self.host, self.port, self.index, self.type = conf.esrtf_init()
		''' __init__(self, hosts=None, transport_class=Transport, **kwargs):
			hosts: list of nodes we should connect to. Node should be a
		    dictionary ({"host": "localhost", "port": 9200}),'''
		try:
			list = [{"host": self.host, "port": self.port}]
			self.es = Elasticsearch(list)
		except Elasticsearch.Error as e:
			print("Elasticsearch Error %d: %s" % (e.args[0], e.args[1]))
	def update(self):
		doc_type = time.strftime("%Y-%m-%d")
		count_dict = self.es.count(index=self.index, doc_type=doc_type)
		count = count_dict['count']
		esrtf_time = time.strftime("%Y-%m-%d %H:%M:%S")
		esrtf_list = [self.host, esrtf_time, count, self.type]
		return esrtf_list
	def updete_site(self, site_name):
		body_s = {"query":  {"multi_match": {"query": site_name, "fields": "site_name"}}}
		doc_type = time.strftime("%Y-%m-%d")
		count_dict = self.es.count(index=self.index, doc_type=doc_type, body=body_s)
		count = count_dict['count']
		return count
	def search_old(self, mysql_conn, str_ex_words, str_in_words):
		q = Q("multi_match", query=str_in_words, fields=['title', 'content'])
		b = Q("multi_match", query=str_ex_words, fields=['title', 'content']) & q
		doc_type = time.strftime("%Y-%m-%d")
		s = Search(index=self.index, doc_type=doc_type).using(self.es).query(analyzer='whitespace').query(b)[0:500]
		# = Search(index=self.index).using(self.es).query(analyzer='whitespace').query(b)[0:500]
		#print doc_type
		response = s.execute()
		return response
	def search_syno(self, mysql_conn, topic_in_words, topic_ex_words, key, list_fields=['title', 'content']):
		list_in = topic_in_words.split(',')
		#print 'topic in words : %s' % topic_in_words
		#print 'topic ex words : %s' % topic_ex_words
		#q = Q("query_string", query='\"%s\"' % list_in[0], fields=['title', 'content'])
		rule = []
		for i in range(len(list_in)):
			q_in = Q("query_string", query='\"%s\"' % list_in[i], fields=list_fields)
			#print list_in[i]
			syno_list = mysql_conn.select_synonymdict_list(list_in[i])
			if len(syno_list) == 0:
				rule.append(q_in)
			else:
				sync_str = syno_list[0]['syno_rela']
				#print 'topic synonym words : %s' % sync_str
				sync_str_list = sync_str.split(',')
				for j in range(len(sync_str_list)):
					p_in = Q("query_string", query='\"%s\"' % sync_str_list[j], fields=list_fields)
					q_in = q_in | p_in
				rule.append(q_in)
		if len(topic_ex_words) > 0:
			list_ex = topic_ex_words.split(',')
			if len(list_ex) > 0:
				q_ex = Q("query_string", query='\"%s\"' % list_ex[0], fields=list_fields)
				for k in range(1, len(list_ex)):
					p_ex = Q("query_string", query='\"%s\"' % list_ex[k], fields=list_fields )
					q_ex = q_ex | p_ex
				rule.append(q_ex)
		query_all = rule[0]
		for m in range(1, len(rule)):
			query_all = query_all & rule[m]
		doc_type = time.strftime("%Y-%m-%d")
		#s = Search(index='isearch6').using(self.es).query(analyzer='whitespace').query(query_all)
		if key == 1:
			s = Search(index=self.index, doc_type=doc_type).using(self.es).query(query_all)
		else:
			s =  Search(index='weibo', doc_type='tweets').using(self.es).query(query_all)
		response = s.execute()
		print 'key: %s ，response.hits.total: %d' % (key,response.hits.total)
		# for hit in response:
		# 	print hit
		# 	print 'content: %s' % hit['Content']
		# 	print 'score: %d' % hit.meta.score
		return response
