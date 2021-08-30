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

totalAmount = []
for word in keywords:
    list_message = set()
    list_description = set()
    body = {
        'size': 00,
        'query': {
            'bool': {
                'must': [
                    {'bool': {
                        'should': [
                            {'match_phrase': {'description': word}},
                            {'match_phrase': {'message': word}}
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

    month = 8
    month = f'{month:02d}'
    for i in range(27, 30):
        day = i
        day = f'{day:02d}'
        index = f'dsminer_post_2021-{month}-{day}'
        checkExcept = True
        while checkExcept == True:
            try:
                response = es.search(index=index, body=body, request_timeout=30)['hits']['hits']
                for res in response:
                    message = res['_source']['message']
                    description = res['_source']['description']
                    list_message.add(message)
                    list_description.add(description)

                print('pass' + ' ' + index)
                print(len(list_message))
                time.sleep(1)
                checkExcept = False
            except:
                print(sys.exc_info()[0])
                print('errol' + ' ' + index)
                time.sleep(time_sleep)
    # else:
    #     month = f'{month:02d}'
    #     for i in range(1, 5):
    #         day = i
    #         day = f'{day:02d}'
    #         index = f'dsminer_post_2021-{month}-{day}'
    #         checkExcept = True
    #         while checkExcept == True:
    #             try:
    #                 response = es.search(index=index, body=body, request_timeout=30)['hits']['hits']
    #                 for res in response:
    #                     userId = res['_source']['ownerRef']['id']
    #                     list_userId.add(userId)
    #
    #                 print('pass' + ' ' + index)
    #                 print(len(list_userId))
    #                 time.sleep(1)
    #                 checkExcept = False
    #             except:
    #                 print(sys.exc_info()[0])
    #                 print('errol' + ' ' + index)
    #                 time.sleep(time_sleep)

    path = r"post_amount.txt"
    out_file = open(path, "w", encoding="utf-8")
    totalAmount = len(list_message)
    for i in range(0, totalAmount):
        out_file.write(str(list_message[i]) + '\n------------------\n'
                       + str(list_description[i]) + '\n------------------------------------\n'
                       )
    out_file.close()

for i in range(0, totalAmount):
    print(i)
