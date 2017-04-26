#!/usr/bin/python
# -*- encoding:utf-8 -*-
import pymongo
import glob
import threading
import re
import traceback

MONGODB_SERVER = "localhost"
MONGODB_PORT = 27017
MONGODB_DB = "motion"

connection = pymongo.MongoClient(
    MONGODB_SERVER,
    MONGODB_PORT
)
db = connection[MONGODB_DB]
collection = db['line_raw_data']
print "Opened database successfully"

raw_data_path = '../raw_data/Line/*.txt'
data_list = glob.glob(raw_data_path)

for file in data_list:
    f = open(file, 'r')
    r = f.readlines()
    date_pattern = ('[A-Z][a-z][a-z]')
    date = ''
    
    for i in r:
        if i == '\n':
            pass
        elif re.match(date_pattern, str(i[:3])):
            date = i
        elif re.match('[0-9][0-9]:[0-9][0-9]', str(i[0:5])):
            cc = i.split()
            content = ''.join(cc[2:]).replace('\n', '').replace('\"', '')
            if cc != []:
                try:
                    collection.insert({'date': date, 'name': cc[1], 'content': content,
                                       'time_stamp': cc[0], 'type': 'Line'})
                except:
                    traceback.print_exc()
