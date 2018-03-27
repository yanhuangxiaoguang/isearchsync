# encoding: utf-8
import os
import re
import jieba
import codecs
#jieba.load_userdict('my_dict.txt')
#mysql_conn = mysql_handler()

sen_dict = {}
f_sen = open('sentiment_score.txt', 'r')
for line in f_sen.readlines():
	line = line.strip('\n')
	line = line.decode('utf-8')
	word = line.split(' ')
	if len(word) == 2:
		sen_dict[word[0]] = word[1]
f_sen.close()
'''否定词'''
not_list = []
f_not = open('notDict.txt', 'r')
for line in f_not.readlines():
	line = line.strip('\n')
	word = line.decode('utf-8')
	not_list.append(word)
f_not.close()
'''程度副词'''
degree_dict = {}
f_degree = open('degreeDict.txt', 'r')
for line in f_degree.readlines():
	line = line.strip('\n')
	line = line.decode('utf-8')
	word = line.split(',')
	if len(word) == 2:
		degree_dict[word[0]] = word[1]
f_degree.close()
'''get stopwords list'''
stopwords = []
f_stop = open('stop_words.txt', 'r')
for line in f_stop.readlines():
	line = line.strip('\n')
	word = line.decode('utf-8')
	stopwords.append(word)
f_stop.close()

def sent2word(title, content):
	""" Segment a sentence to words Delete stopwords """
	'''以， 。 ? !分句，并将title加入sentence list '''
	cut_word = '，|。|？|！'
	cut_word = cut_word.decode('utf-8')
	cut_list = re.split(cut_word, content)
	list_sentence = []
	for i in range(len(cut_list)):
		if len(cut_list[i])>3:
			list_sentence.append(cut_list[i])
			#print cut_list[i]
	list_sentence.append(title)
	#print len(list_sentence)
	reslut_list = []
	for i in range(len(list_sentence)):
		sentence = list_sentence[i]
		segList = jieba.cut(sentence)
		segResult = []
		for w in segList:
			segResult.append(w)
		'''remove stopwords'''
		newSent = []
		for word in segResult:
			if word in stopwords:
				#print "stopword: %s" % word
				continue
			else:
				newSent.append(word)
		#print newSent
		#print len(newSent)
		reslut_list.append(newSent)
	return reslut_list
def classifyWords(wordlist):
	'''将分词后的词语列表，转变为与位置相关的词典'''
	word_dict = {}
	for i in range(len(wordlist)):
		word_dict[wordlist[i]] = i
	sen_word_dict = {}
	not_word_dict = {}
	degree_word_dict = {}
	for word in word_dict.keys():
		if word in sen_dict.keys() and word not in not_list and word not in degree_dict.keys():
			sen_word_dict[word_dict[word]] = sen_dict[word]
		if word in not_list and word not in degree_dict.keys():
			not_word_dict[word_dict[word]] = -1
		elif word in degree_dict.keys():
			degree_word_dict[word_dict[word]] = degree_dict[word]
	#print sen_word_dict, not_word_dict, degree_word_dict
	return sen_word_dict, not_word_dict, degree_word_dict
def score_sent(sen_word, not_word, deg_word, sen_len):
	w = 1
	score = 0
	'''情感词，否定词，程度词 在句子 的位置 的列表'''
	sen_list = list(sen_word.keys())
	not_list = list(not_word.keys())
	deg_list = list(deg_word.keys())
	#print sen_list
	#print not_list
	#print deg_list
	senloc = -1
	for i in range(sen_len):
		if i in sen_list:
			senloc += 1
			score += w * float(sen_word[i])
			if senloc < (len(sen_list)-1):
				'''判断情感之间有不有not degree词'''
				for j in range(sen_list[senloc], sen_list[senloc+1]):
					if j in not_list:
						w *= -1
					elif j in deg_list:
						w *= float(deg_word[j])
		if senloc < (len(sen_list)-1):
			i = sen_list[senloc + 1]
	#print score
	'''如果句子超长，将以23个词为一句，平均分数'''
	if sen_len>39:
		score  = score/int(sen_len/20)
	return score
def emotion(title, content):
		list_sent = sent2word(title, content)
		score_list = []
		score_all = 0
		for j in range(len(list_sent)):
			sen_word_dict, not_word_dict, degree_word_dict = classifyWords(list_sent[j])
			score = score_sent(sen_word_dict, not_word_dict, degree_word_dict, len(list_sent[j]))
			score_list.append(score)
			#score_all += score
		#print "allllllllllllllll"
		#score_all = 0
		num = len(score_list)
		if num == 2:
			score_all = score_list[0]*(1/3) + score_list[1]*(2/3)
		elif num > 2:
			for i in range(num-1):
				score_all += (score_list[i] * (num-2))/((num-1)*(num-1))
			score_all += score_list[num-1] * (1/(num-1))
		#print sentence_list[i]['id']
		print score_all
		if score_all< -0.35:
			return -1
		elif score_all < 5:
			return 0
		else:
			return 1