#encoding=utf-8
import jieba
str ='10996	圣罗兰口红西南科技大学，西南科大各种欧美化妆品俄罗斯零食代我现西科大一新生朋以私我发给你关件再商议'
jieba.load_userdict('my_dict.txt')
def cut_word(str):
	seg_list = jieba.cut(str, cut_all=False)
	print type(seg_list)
	print "Default Mode:", "/ ".join(seg_list) #精确模式

def join_word(word, freq=None, tag=None):
	jieba.add_word(word=word, freq=freq, tag=tag)

cut_word(str)
join_word('口红西', 10, 'n')
cut_word(str)