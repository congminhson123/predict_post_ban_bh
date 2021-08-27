import time
import sys
from elasticsearch import Elasticsearch

es = Elasticsearch('http://103.74.122.196:9200')

time_sleep = 1
keywords = ['thương mại',
 'nhân thọ',
 'sức khỏe',
 'tai nạn',
 'y tế',
 'phi nhân thọ',
 'tài sản',
 'thiệt hại',
 'hàng hóa',
 'hàng không',
 'xe cơ giới',
 'cháy nổ',
 'tàu',
 'trách nhiệm',
 'tín dụng',
 'tài chính',
 'kinh doanh',
 'nông nghiệp',
 'tiền gửi',
 'xã hội']




result = []
for word in keywords:
    list_userId = set()
    body = {
        'size': 10000,
        'query': {
            'bool': {
                'must': [
                    {'bool': {
                        'should': [
                            {'match_phrase': {'description': word}},
                            {'match_phrase': {'message':word}}
                        ]
                    }},
                    {'bool': {
                        'should': [
                            {'match_phrase': {'description': 'bảo hiểm'}},
                            {'match_phrase': {'message': 'bảo hiểm'}}
                        ]
                    }},
                    # {'match_phrase': {
                    #     'docType': 'user_post'
                    # }}
                ],
                # 'must_not': {
                #     'match_phrase': {
                #         'docType': 'page_post'
                #     }
                # }
            }
        },
        'track_total_hits': True,
        # '_source': [
        #
        # ]
    }

    for month in range(7, 9):
        if month == 7:
            month = f'{month:02d}'
            for i in range(1, 32):
                day = i
                day = f'{day:02d}'
                index = f'dsminer_post_2021-{month}-{day}'
                checkExcept = True
                while checkExcept == True:
                    try:
                        response = es.search(index=index, body=body, request_timeout=30)['hits']['hits']
                        for res in response:
                            userId = res['_source']['ownerRef']['id']
                            list_userId.add(userId)

                        print('pass' + ' ' + index)
                        print(len(list_userId))
                        time.sleep(1)
                        checkExcept = False
                    except:
                        print(sys.exc_info()[0])
                        print('errol' + ' ' + index)
                        time.sleep(time_sleep)
        else:
            month = f'{month:02d}'
            for i in range(1, 5):
                day = i
                day = f'{day:02d}'
                index = f'dsminer_post_2021-{month}-{day}'
                checkExcept = True
                while checkExcept == True:
                    try:
                        response = es.search(index=index, body=body, request_timeout=30)['hits']['hits']
                        for res in response:
                            userId = res['_source']['ownerRef']['id']
                            list_userId.add(userId)

                        print('pass' + ' ' + index)
                        print(len(list_userId))
                        time.sleep(1)
                        checkExcept = False
                    except:
                        print(sys.exc_info()[0])
                        print('errol' + ' ' + index)
                        time.sleep(time_sleep)
    result.append(len(list_userId))
    path = r"C:\Users\acer\Documents\hoc tap\thuc tap bao hiem\check độ phủ\post_amount.txt"
    out_file = open(path, "w", encoding="utf-8")
    for i in result:
        out_file.write(str(i) + '\n')
    out_file.close()

for i in result:
    print(i)

