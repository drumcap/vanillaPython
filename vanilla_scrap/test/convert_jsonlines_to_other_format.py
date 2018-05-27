# -*- coding:utf-8 -*-
import json
import os
import time

import simplejson
import tablib


def sort_data(l):
    l = sorted(l, key=lambda x: x['review_id'])
    # l = sorted(l, key=lambda x: x['date'])
    return l


def remove_duplicate(l):
    s = set()
    for d in l:
        tmp = frozenset(d.items())
        s.add(tmp)

    clean_l = []
    for i in s:
        d = {k: v for k, v in i}
        clean_l.append(d)
    return clean_l

def transfer_data(d):
    d['review_id'] = int(d['review_id'])
    d['rating'] = int(d['rating'])
    return d

def list_of_dict_to_dataset(l):
    data = tablib.Dataset()
    data.headers = 'review_id', 'rating', 'movie_id', 'movie_name', 'review_txt', 'author', 'source', 'date'
    for d in l:
        data.append([d[h] for h in data.headers])
    return data


def load_jsonlines():
    with open('items-top50.jl', 'r') as f:
        r = f.readlines()
    return [simplejson.loads(i) for i in r]

def write_jsonlines(l):
    with open('items-write.jl', 'w', encoding='utf-8') as f:
        for d in l:
            f.write(json.dumps(d, ensure_ascii=False, sort_keys=True) + "\n")


s_time = time.time()

l = load_jsonlines()
l = remove_duplicate(l)
l = list(map(transfer_data, l))
l = sort_data(l)
write_jsonlines(l)

data = list_of_dict_to_dataset(l)

os.makedirs('result', mode=0o777, exist_ok=True)

with open('result/result.xls', 'wb') as o:
    o.write(data.xls)

with open('result/result.csv', 'w') as o:
    o.write(data.csv)

with open('result/result.tsv', 'w') as o:
    o.write(data.tsv)

with open('result/result.json', 'w') as o:
    o.write(data.json)

with open('result/result.yaml', 'w') as o:
    o.write(data.yaml)

print("--- {} 소요됨 ---".format(time.time() - s_time))