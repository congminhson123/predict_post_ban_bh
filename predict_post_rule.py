import time
import sys
from elasticsearch import Elasticsearch
from pandas import DataFrame



keywords = []
mustnot = []
kw = open("keywords.txt", encoding="utf-8")
mn = open("mustnot.txt", encoding="utf-8")
for i in kw:
    keywords.append(i.strip())
for i in mn:
    mustnot.append(i.strip())
def is_ban_bh(s):
    s = s.lower()
    for i in keywords:
        if i in s:
            for j in mustnot:
                if j in s:
                    print("0")
                    return
            print("1")
            return
    print("0")

# is_ban_bh("Công ty bảo hiểm nhân thọ Manulife Việt Nam cung cấp một danh mục các sản phẩm bảo hiểm đa dạng gồm bảo hiểm sức khỏe, bảo hiểm tai nạn, bảo hiểm cho bé, bảo hiểm hưu trí... công ty bảo hiểm, bảo hiểm đai ichi life, bao hiem prudential...")
