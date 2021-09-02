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
user_id = list()
body = {
    'size': 10000,
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
    '_source': [
        'userId', 'message', 'description'
    ]
}

for i in keywords:
    body["query"]["bool"]["must"][0]["bool"]["should"].append({"match_phrase": {"message": i}})
    body["query"]["bool"]["must"][0]["bool"]["should"].append({"match_phrase": {"description": i}})
for i in mustnot:
    body["query"]["bool"]["must_not"][0]["bool"]["should"].append({"match_phrase": {"message": i}})
    body["query"]["bool"]["must_not"][0]["bool"]["should"].append({"match_phrase": {"description": i}})



month = 8
month = f'{month:02d}'
for i in range(1, 32    ):
    day = i
    day = f'{day:02d}'
    index = f'dsminer_post_2021-{month}-{day}'
    checkExcept = True
    while checkExcept:
        try:
            response = es.search(index=index, body=body, request_timeout=30)['hits']['hits']
            for res in response:
                message = res['_source']['message'].lower()
                description = res['_source']['description'].lower()
                if message is not None:
                    list_post.append(message)
                    user_id.append(res['_source']['userId'])
                if description is not None:
                    list_post.append(description)
                    user_id.append(res['_source']['userId'])

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

df = DataFrame({'userId':user_id, 'post':list_post})
df = df.drop_duplicates()
df['post'] = df.groupby(['userId'])['post'].transform(lambda x : ', '.join(x))
df = df.drop_duplicates()
classify_kw= []
cls_kw = open("classify_keyword.txt", encoding="utf-8")
for i in cls_kw:
    classify_kw.append(i.strip())
df['loại bảo hiểm'] = ""
df['công ty bảo hiểm'] = ""
for index, row in df.iterrows():
    type_bh = []
    cty_bh = []
    for i in classify_kw[0:8]:
        if i in row['post']:
            type_bh.append('bảo hiểm y tế')
            break
    for i in classify_kw[8:16]:
        if i in row['post']:
            type_bh.append('bảo hiểm nhân thọ')
            break
    for i in classify_kw[16:33]:
        if i in row['post']:
            type_bh.append('bảo hiểm sức khỏe')
            break
    for i in classify_kw[33:41]:
        if i in row['post']:
            type_bh.append('bảo hiểm thân thể')
            break
    for i in classify_kw[41:59]:
        if i in row['post']:
            type_bh.append('bảo hiểm xe cơ giới')
            break
    for i in classify_kw[59:67]:
        if i in row['post']:
            type_bh.append('bảo hiểm xã hội')
            break
    for i in classify_kw[67:71]:
        if i in row['post']:
            type_bh.append('bảo hiểm tài chính')
            break
    for i in classify_kw[71:74]:
        if i in row['post']:
            type_bh.append('bảo hiểm kinh doanh')
            break
    for i in classify_kw[74:79]:
        if i in row['post']:
            type_bh.append('bảo hiểm tai nạn')
            break
    for i in classify_kw[79:90]:
        if i in row['post']:
            type_bh.append('bảo hiểm tài sản')
            break
    for i in classify_kw[96:100]:
        if i in row['post']:
            type_bh.append('bảo hiểm hàng không')
            break
    for i in classify_kw[100:107]:
        if i in row['post']:
            type_bh.append('bảo hiểm thương mại')
            break
    for i in classify_kw[107:111]:
        if i in row['post']:
            type_bh.append('bảo hiểm hàng hóa')
            break
    for i in classify_kw[111:116]:
        if i in row['post']:
            type_bh.append('bảo hiểm thiệt hại')
            break
    for i in classify_kw[116:119]:
        if i in row['post']:
            type_bh.append('bảo hiểm nông nghiệp')
            break
    for i in classify_kw[119:125]:
        if i in row['post']:
            type_bh.append('bảo hiểm phi nhân thọ')
            break
    for i in classify_kw[125:128]:
        if i in row['post']:
            type_bh.append('bảo hiểm cháy nổ')
            break
    for i in classify_kw[128:132]:
        if i in row['post']:
            type_bh.append('bảo hiểm tổng hợp')
            break
    while True:
        if 'manulife' in row['post']:
            cty_bh.append('manulife')
            break
        for i in classify_kw[132:139]:
            if i in row['post']:
                cty_bh.append('bảo việt')
                break
        if 'aia' in row['post']:
            cty_bh.append('aia')
            break
        if 'vbi' in row['post']:
            cty_bh.append('vbi')
            break
        if ('cathaylife' or 'cathay life') in row['post']:
            cty_bh.append('cathay life')
            break
        if ("dai-ichi" or "dai ichi" or "daiichi") in row['post']:
            cty_bh.append('dai-ichi')
            break
        if 'aviva' in row['post']:
            cty_bh.append('aviva')
            break
        if 'fwd' in row['post']:
            cty_bh.append('fwd')
            break
        if 'pvi' in row['post']:
            cty_bh.append('pvi')
            break
        if ('mb ageas' or 'mbageas') in row['post']:
            cty_bh.append('mb ageas')
            break
        if 'prudential' in row['post']:
            cty_bh.append('prudential')
            break
        if 'generali' in row['post']:
            cty_bh.append('generali')
            break
        if ('chubblife' or 'chubb life') in row['post']:
            cty_bh.append('chubb life')
            break
        if 'hanwha' in row['post']:
            cty_bh.append('hanwha')
            break
        if ('sunlife' or 'sun life') in row['post']:
            cty_bh.append('sun life')
            break
        for i in classify_kw[159:165]:
            if i in row['post']:
                cty_bh.append('bảo minh')
                break
        for i in classify_kw[165:171]:
            if i in row['post']:
                cty_bh.append('phú hưng')
                break
        for i in classify_kw[171:174]:
            if i in row['post']:
                cty_bh.append('bidv metlife')
                break
        break
    if not type_bh: type_bh.append('other')
    if not cty_bh: cty_bh.append('other')

    row['loại bảo hiểm'] = ', '.join(str(s) for s in type_bh)
    row['công ty bảo hiểm'] = ', '.join(str(s) for s in cty_bh)
df = df.drop('post', 1)
df.reset_index(drop=True, inplace=True)

print(df)

df.to_excel(r'classify_user.xlsx', encoding='utf-8')

