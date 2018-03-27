'''from mysql_init import mysql_handler
from esrtf_init import esrtf_handler
mysql_conn = mysql_handler()
esrtf_conn = esrtf_handler()
content_list = mysql_conn.select_content()
for i in range(len(content_list)):
	section_name = esrtf_conn.search_url(content_list[i]['url'])
	if section_name:
		mysql_conn.update_webpage_section_name(id=content_list[i]['id'], section_name=section_name)
'''
