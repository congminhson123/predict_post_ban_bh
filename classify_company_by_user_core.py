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
user_id = list()
list_user_work = list()
body = {
    #     'from': 0,
    'size': 10000,
    'query': {
        'bool': {
            'must': [
                {'bool': {
                    'should': [

                    ]
                }}

            ],
            'must_not': [

            ]
        }
    },
    'track_total_hits': True,
    '_source': [
        'id', 'works',

    ]
}
for i in keywords:
    body["query"]["bool"]["must"][0]["bool"]["should"].append({"match_phrase": {"works.employer": i}})
    body["query"]["bool"]["must"][0]["bool"]["should"].append({"match_phrase": {"works.position": i}})
try:
    res = es.search(index="dsminer_user_core", body=body, scroll='1s')
    old_scroll_id = res['_scroll_id']  # id của lần scroll

    while len(res['hits']['hits']):

        for doc in res['hits']['hits']:
            works = doc['_source']['works']
            if len(works) > 1:
                for work in works:
                    position = work['position']
                    employer = work['employer']

                    if position is not None:
                        list_user_work.append(position.lower())
                        user_id.append(doc['_source']['id'])
                    if employer is not None:
                        list_user_work.append(employer.lower())
                        user_id.append(doc['_source']['id'])

        # request sử dụng scroll api
        res = es.scroll(
            scroll_id=old_scroll_id,
            scroll='1s'
        )
        old_scroll_id = res['_scroll_id']
except:
    print(sys.exc_info()[0])
    time.sleep(time_sleep)
df = DataFrame({'user_id': user_id, 'works': list_user_work})
# nan_value = float("NaN")
# df.replace("", nan_value, inplace=True)
# df = df.dropna(axis=0, subset=['works'])
# print(df)
#
# df.to_excel(r'classify_company_by_user_core.xlsx', encoding='utf-8')
df = df.drop_duplicates()
df['works'] = df.groupby(['user_id'])['works'].transform(lambda x: ', '.join(x))
df = df.drop_duplicates()
classify_kw = []
cls_kw = open("classify_keyword.txt", encoding="utf-8")
for i in cls_kw:
    classify_kw.append(i.strip())
df['công ty bảo hiểm'] = ""
df['loại bảo hiểm'] = ""
for index, row in df.iterrows():
    company = []
    type = []
    for i in classify_kw[0:8]:
        if i in row['works']:
            type.append('bảo hiểm y tế')
            break
    for i in classify_kw[8:16]:
        if i in row['works']:
            type.append('bảo hiểm nhân thọ')
            break
    for i in classify_kw[16:33]:
        if i in row['works']:
            type.append('bảo hiểm sức khỏe')
            break
    for i in classify_kw[33:41]:
        if i in row['works']:
            type.append('bảo hiểm thân thể')
            break
    for i in classify_kw[41:59]:
        if i in row['works']:
            type.append('bảo hiểm xe cơ giới')
            break
    for i in classify_kw[59:67]:
        if i in row['works']:
            type.append('bảo hiểm xã hội')
            break
    for i in classify_kw[67:71]:
        if i in row['works']:
            type.append('bảo hiểm tài chính')
            break
    for i in classify_kw[71:74]:
        if i in row['works']:
            type.append('bảo hiểm kinh doanh')
            break
    for i in classify_kw[74:79]:
        if i in row['works']:
            type.append('bảo hiểm tai nạn')
            break
    for i in classify_kw[79:90]:
        if i in row['works']:
            type.append('bảo hiểm tài sản')
            break
    for i in classify_kw[96:100]:
        if i in row['works']:
            type.append('bảo hiểm hàng không')
            break
    for i in classify_kw[100:107]:
        if i in row['works']:
            type.append('bảo hiểm thương mại')
            break
    for i in classify_kw[107:111]:
        if i in row['works']:
            type.append('bảo hiểm hàng hóa')
            break
    for i in classify_kw[111:116]:
        if i in row['works']:
            type.append('bảo hiểm thiệt hại')
            break
    for i in classify_kw[116:119]:
        if i in row['works']:
            type.append('bảo hiểm nông nghiệp')
            break
    for i in classify_kw[119:125]:
        if i in row['works']:
            type.append('bảo hiểm phi nhân thọ')
            break
    for i in classify_kw[125:128]:
        if i in row['works']:
            type.append('bảo hiểm cháy nổ')
            break
    for i in classify_kw[128:132]:
        if i in row['works']:
            type.append('bảo hiểm tổng hợp')
            break
    if not type: type.append('other')
    while True:
        if 'manulife' in row['works']:
            company.append('manulife')
            break
        for i in classify_kw[132:139]:
            if i in row['works']:
                company.append('bảo việt')
                break
        if 'aia' in row['works']:
            company.append('aia')
            break
        if 'vbi' in row['works']:
            company.append('vbi')
            break
        if ('cathaylife' or 'cathay life') in row['works']:
            company.append('cathay life')
            break
        if ("dai-ichi" or "dai ichi" or "daiichi") in row['works']:
            company.append('dai-ichi')
            break
        if 'aviva' in row['works']:
            company.append('aviva')
            break
        if 'fwd' in row['works']:
            company.append('fwd')
            break
        if 'pvi' in row['works']:
            company.append('pvi')
            break
        if ('mb ageas' or 'mbageas') in row['works']:
            company.append('mb ageas')
            break
        if 'prudential' in row['works']:
            company.append('prudential')
            break
        if 'generali' in row['works']:
            company.append('generali')
            break
        if ('chubblife' or 'chubb life') in row['works']:
            company.append('chubb life')
            break
        if 'hanwha' in row['works']:
            company.append('hanwha')
            break
        if ('sunlife' or 'sun life') in row['works']:
            company.append('sun life')
            break
        for i in classify_kw[159:165]:
            if i in row['works']:
                company.append('bảo minh')
                break
        for i in classify_kw[165:171]:
            if i in row['works']:
                company.append('phú hưng')
                break
        for i in classify_kw[171:174]:
            if i in row['works']:
                company.append('bidv metlife')
                break
        break
    if not company: company.append('other')
    row['loại bảo hiểm'] = ', '.join(str(s) for s in type)
    row['công ty bảo hiểm'] = ', '.join(str(s) for s in company)
df.reset_index(drop=True, inplace=True)
print(df)
df.to_excel(r'classify_company_by_user_core.xlsx', encoding='utf-8')
