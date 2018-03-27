#encoding=utf-8
import jieba
from jieba import analyse
import sys
sys.path.append("../")
jieba.load_userdict('my_dict.txt')

def top_word(text):
	num = (len(text))/30
	if num > 20:
		num = 20
	if num < 3:
		num = 3
	#print num
	result = jieba.analyse.textrank(text, topK=num, withWeight=True, allowPOS=('ns', 'n', 'vn', 'nr', 'nt', 'ns'))
	set_1 = set()
	for k, v in result:
		#print '%s :%s ' % (k, v)
		set_1.add(k)
	set_2 = set()
	result = jieba.analyse.extract_tags(text, topK=num, withWeight=True, allowPOS=('ns', 'n', 'vn', 'nr', 'nt', 'ns'))
	for k, v in result:
		#print '%s :%s ' % (k, v)
		set_2.add(k)
	key_word = set_1.intersection(set_2)
	#print key_word
	key_word_str = ''
	for i in key_word:
		key_word_str = key_word_str + i + ','
	print key_word_str
	return key_word_str
