import os
import time

"""
kv_pair_0 = [i+8 for i in range(11)]

for i in kv_pair_0:
    print("CryptDB: "+str(i))
    os.system("mysqlslap -uroot -pletmein -h0.0.0.0 -P49153 --concurrency=1 --number-of-queries=2000 --create-schema=KV_test --query=\"select myvalue from mykv"+str(pow(2, i))+" where mykey = 'aaaaaajg'\" >> 5_3_2.txt 2>&1")
    time.sleep(10)
"""

kv_pair_0 = [i+8 for i in range(11)]

for i in kv_pair_0:
    print("CryptDB range query: "+str(i))
    os.system("mysqlslap -uroot -pletmein -h0.0.0.0 -P49153 --concurrency=1 --number-of-queries=2000 --create-schema=KV_test --query=\"select myvalue from mykv"+str(pow(2, i))+" where mykey between 'aaaaaaac' and 'aaaaabzz'\" >> 5_3_3.txt 2>&1")
    time.sleep(5)

r_list = [i+9 for i in range(16)]

for i in r_list:
    print("HElib: "+str(i))
    os.system("../HElib/build/bin/main "+str(i)+" >> 5_4.txt 2>&1")
    time.sleep(5)

