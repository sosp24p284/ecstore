
import os
import time

#kv_list = [i+8 for i in range(17)]
#kv_list = [16, 17, 18, 19, 20]
kv_list = [20, 18, 16, 14, 12, 10, 8]
repeat = 5

for o in range(repeat):
    for i in kv_list:
        time.sleep(10)
        os.system("nohup ./server/build/server >> plaintext_server_8.txt 2>&1 &")
        time.sleep(20)
        os.system("./client/build/client "+str(i)+" >> plaintext_client_8.txt 2>&1")
        time.sleep(30)
        #os.system("rm hotee_wiredtiger_server_4.txt")
        os.system("rm -r WT_HOME")