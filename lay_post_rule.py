import time
import sys
from elasticsearch import Elasticsearch
from pandas import DataFrame

es = Elasticsearch('http://103.74.122.196:9200')

time_sleep = 1
keywords = []
mustnot = []
kw = open("keywords.txt", encoding="utf-8")
mn = open("mustnot.txt", encoding="utf-8")
for i in kw:
    keywords.append(i.strip())
for i in mn:
    mustnot.append(i.strip())

list_post = list()
docId_post = list()
user_id = set()
body = {
    'size': 200,
    'query': {
        'bool': {
            'must': [
                {'bool': {
                        'should': [

                        ]
                }},
                {'match_phrase': {
                    'docType': 'user_post'
                }}
            ],
            'must_not': [
                {'bool': {
                        'should': [

                        ]
                }},
            ]
        }
    },
    'track_total_hits': True,

}

for i in keywords:
    body["query"]["bool"]["must"][0]["bool"]["should"].append({"match_phrase": {"message": i}})
    body["query"]["bool"]["must"][0]["bool"]["should"].append({"match_phrase": {"description": i}})
for i in mustnot:
    body["query"]["bool"]["must_not"][0]["bool"]["should"].append({"match_phrase": {"message": i}})
    body["query"]["bool"]["must_not"][0]["bool"]["should"].append({"match_phrase": {"description": i}})



month = 8
month = f'{month:02d}'
for i in range(27, 32):
    day = i
    day = f'{day:02d}'
    index = f'dsminer_post_2021-{month}-{day}'
    checkExcept = True
    while checkExcept:
        try:
            response = es.search(index=index, body=body, request_timeout=30)['hits']['hits']
            for res in response:
                message = res['_source']['message']
                description = res['_source']['description']
                docId = res['_source']['docId']
                # user_id.add(res['_source']['userId'])
                if message is not None:
                    list_post.append(message)
                    docId_post.append(docId)
                if description is not None:
                    list_post.append(description)
                    docId_post.append(docId)

            # print('pass' + ' ' + index)
            # print(len(list_message))
            time.sleep(1)
            checkExcept = False
        except:
            print(sys.exc_info()[0])
            print('errol' + ' ' + index)
            time.sleep(time_sleep)
# path = r"post_amount.txt"
# out_file = open(path, "w", encoding="utf-8")
# list_data = []
# messages = list(list_message)
# descriptions = list(list_description)
# totalAmount = len(descriptions)
# for i in range(0, totalAmount):
#     data = ('MESSAGE\n' + str(messages[i])
#             +  '\nDESCRIPTION\n' + str(descriptions[i])
#             )
#     list_data.append(data)
#     print(i)
# print(len(user_id))
# with open('user_id.txt', 'w') as f:
#     for item in user_id:
#         f.write(item+"\n")
df = DataFrame({'docId_post':docId_post, 'post':list_post})
nan_value = float("NaN")
df.replace("", nan_value, inplace=True)
df = df.dropna(axis=0, subset=['post'])
print(df)
df.to_excel(r'data_rule.xlsx', encoding='utf-8')

