# -*- coding:utf-8 -*-
import json
import time
import simplejson

def transfer_data(d):
    d['review_id'] = int(d['review_id'])
    d['rating'] = int(d['rating'])
    return d

s_time = time.time()
with open('items-top50.jl','r') as input_file:
    with open("items-write.jl","w") as out_file:
        [
            # json 데이터 저장
            # out_file.write(
            #     json.dumps(transfer_data(d), ensure_ascii=False, sort_keys=True) + "\n"
            # )

            # text 데이터 저장
            out_file.write(d["review_txt"] + "\n")

            for d in [
                simplejson.loads(i)
                    for i in input_file.readlines()
            ]

            # 조건추가
            if int(d['rating']) < 9
        ]
print("--- {} 소요됨 ---".format(time.time() - s_time))