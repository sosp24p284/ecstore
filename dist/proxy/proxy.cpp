#include <sys/types.h>
#include <sys/socket.h>
#include <stdio.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <sys/shm.h>
#include <iostream>
#include <thread>

#include <cstdlib>
#include <cstring>
#include <cmath>
#include <vector>
#include <map>
//#include <iostream>
#include <chrono>
#include <algorithm>
#include <wiredtiger.h>

#define IP "127.0.0.1"
#define length_unit_bloom 10
#define num_hash 3
#define max_rand 10000
#define k_byte 8
#define v_byte 64
#define qs 1018081
#define ts 841

using namespace std;

int PORT;
int s;
struct sockaddr_in servaddr;
socklen_t len;
std::vector<int> li;//用list来存放套接字，没有限制套接字的容量就可以实现一个server跟若干个client通信
bool run = true;
bool run2 = true;

auto start = std::chrono::steady_clock::now();
auto stop = std::chrono::steady_clock::now();
auto start1 = std::chrono::steady_clock::now();
auto stop1 = std::chrono::steady_clock::now();
auto start2 = std::chrono::steady_clock::now();
auto stop2 = std::chrono::steady_clock::now();
auto total_ps1 = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
auto total_rs1 = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
auto total_ps2 = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
auto total_rs2 = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();

auto total_filter = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
auto total_enc = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
auto total_dec = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
auto total_ks = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
int num_ps = 0;
int num_rs = 0;
int num_filter = 0;
int num_enc = 0;
int num_dec = 0;
int num_ks = 0;

void Stringsplit(const string& str, const char split, vector<string>& res)
{
    //cout << "Check: 1"<< endl;
    if (str == "")        return;
    //cout << "Check: 2"<< endl;
    //在字符串末尾也加入分隔符，方便截取最后一段
    string strs = str + split;
    //cout << "Check: 3"<< endl;
    size_t pos = strs.find(split);
    //cout << "Check: 4"<< endl;
    // 若找不到内容则字符串搜索函数返回 npos
    while (pos != strs.npos)
    {
        string temp = strs.substr(0, pos);
        res.push_back(temp);
        //去掉已分割的字符串,在剩下的字符串中进行分割
        strs = strs.substr(pos + 1, strs.size());
        pos = strs.find(split);
    }
    //cout << "Check: 5"<< endl;
};

map<string, int>parser_case = {
    {"insert", 1},
    {"update", 2},
    {"pointsearch", 3},
    {"rangesearch", 4},
    {"delete", 5}
};

class filter_generator {
    private:
        int sensitivity;
        int k;
        int b;
        int bound_hash;
        vector<int> hash_key;
    public:
        filter_generator(int s): sensitivity(s) {
            k = rand() % max_rand;
            b = rand() % max_rand;
            bound_hash = k * sensitivity - length_unit_bloom + num_hash;
            hash_key.push_back((rand() % int(k * sensitivity)));
            for(int i = 0; i < num_hash; i++) {
                hash_key.push_back((rand() % max_rand));
            }
        };
        string get_filter(int x) {
            int offset = k * x + b + (int(x) % int(hash_key[0]));
            int range;
            int* filter = new int[num_hash];
            for (int i = 0; i < num_hash; i++) {
                filter[i] = (int(x) % int(hash_key[i+1])) % length_unit_bloom;
            }
            sort(filter, filter+num_hash);
            range = filter[0] + offset;
            //range[1] = range[0] + length_unit_bloom;
            for (int i = 0; i < num_hash; i++) {
                filter[i] = filter[i] ^ offset;
            }
            sort(filter, filter+num_hash);
            string temp = to_string(range);
            int num_padding = 8 - temp.length();
            if (0 < num_padding--) {
                temp = "0" + temp;
            };
            for (int i=0; i < num_hash; i++) {
                temp = temp + to_string(filter[i]);
            }
            //return std::make_tuple(range, filter);
            //cout << "The filter is: " << temp << endl;
            return temp;
        };
        string get_filter(const string & x) {
            start2 = std::chrono::steady_clock::now();
            int x_num = 0;
            for (const auto & ch : x) {
                x_num = x_num * 26 + ch;
            };
            //cout << "The serialized number is: " << x_num << endl;
            string temop = get_filter(x_num);
            stop2 = std::chrono::steady_clock::now();
            total_filter += std::chrono::duration_cast<std::chrono::nanoseconds>(stop2 - start2).count();
            num_filter += 1;
            return temop;
        };

};

filter_generator fltr_gen(1);

class bgv {
    private:
        int a;
        int b;
        int s;
        int t;
        int e;
        int q;
        int s2;
        int a2;
        string temp_a;
        string temp_a2;
    public:
        bgv() {
            t = 29;
            a = 13;
            a2 = 12;
            s = 15;
            temp_a = "0013";
            temp_a2 = "0012";
            q = 1009;
            s2 = 14;
        };
        string encrypt(string ptxt) {
            start1 = std::chrono::steady_clock::now();
            string temp_b;
            string result;
            for (int i = 0; i < ptxt.length(); i++) {
                temp_b = to_string((qs + a * s + (int(ptxt[i]) - 97) + t * (rand() % 15)) % q);
                while (temp_b.length() < 4) {
                    temp_b = "0"+temp_b;
                };
                result += temp_b;
                result += temp_a;
            }
            stop1 = std::chrono::steady_clock::now();
            total_enc += std::chrono::duration_cast<std::chrono::nanoseconds>(stop1 - start1).count();
            num_enc += 1;
            return result;
        };
        string decrypt1(string ctxt) {
            start1 = std::chrono::steady_clock::now();
            string result;
            int num = ctxt.length() / 8;
            for (int i = 0; i < num; i++) {
                string temp_b = ctxt.substr(8*i, 4);
                string temp_a0 = ctxt.substr(8*i+4, 4);
                result.push_back(static_cast<char>(97 + ((ts+qs+atoi(temp_b.c_str()) - atoi(temp_a0.c_str()) * s)% q % t)));
            }
            stop1 = std::chrono::steady_clock::now();
            total_dec += std::chrono::duration_cast<std::chrono::nanoseconds>(stop1 - start1).count();
            num_dec += 1;
            return result;
        };
        string decrypt2(string ctxt) {
            start1 = std::chrono::steady_clock::now();
            string result;
            int num = ctxt.length() / 8;
            for (int i = 0; i < num; i++) {
                string temp_b = ctxt.substr(8*i, 4);
                string temp_a0 = ctxt.substr(8*i+4, 4);
                result.push_back(static_cast<char>(97 + ((qs+ts+atoi(temp_b.c_str()) - atoi(temp_a0.c_str()) * s2) % q % t)));
            }
            stop1 = std::chrono::steady_clock::now();
            total_dec += std::chrono::duration_cast<std::chrono::nanoseconds>(stop1 - start1).count();
            num_dec += 1;
            return result;
        };
        string keyswitch(string ctxt) {
            start1 = std::chrono::steady_clock::now();
            string result;
            int num = ctxt.length() / 8;
            for (int i = 0; i < num; i++) {
                string temp_b = ctxt.substr(8*i, 4);
                string temp_a0 = ctxt.substr(8*i+4, 4);
                string temp_b_new = to_string((qs+atoi(temp_b.c_str())-atoi(temp_a0.c_str())*(a2*s2+s+t*(10)))%q);
                while (temp_b_new.length() < 4) {
                    temp_b_new = "0"+temp_b_new;
                };
                string temp_a_new = to_string((qs-atoi(temp_a0.c_str())*a2)%q);
                while (temp_a_new.length() < 4) {
                    temp_a_new = "0"+temp_a_new;
                };
                result += temp_b_new;
                result += temp_a_new;
            }
            stop1 = std::chrono::steady_clock::now();
            total_ks += std::chrono::duration_cast<std::chrono::nanoseconds>(stop1 - start1).count();
            num_ks += 1;
            return result;
        };
};

bgv my_bgv;

string parser(string query) {
    //cout << "Sub-step: 1" << endl;
    vector<string> sentences;
    //cout << "Sub-step: 2" << endl;
    query.pop_back();
    //cout << "Sub-step: 3" << endl;
    //Stringsplit(query, '\n', sentences);
    //cout << "Sub-step: 4" << endl;
    string query_result;
    //cout << "Sub-step: 5" << endl;
    //for (int o = 0; o < sentences.size(); o++) {
        //cout << "Sub-step: 6" << endl;
        vector<string> strList;
        //cout << "Sub-step: 7" << endl;
        Stringsplit(query, ';', strList);
        //cout << "Sub-step: 8" << endl;
        /*
        cout << "\n\n" << endl;
        for(int i = 0; i < strList.size(); i++) {
            cout << "->>>>>receiving>" << strList[i] << "<" << endl;
        }
        */
        //cout << "Sub-step: 9" << endl;
        switch(parser_case[strList[0]]) {
            case 1:
                query_result = "insert;"+fltr_gen.get_filter(strList[1])+";"+my_bgv.encrypt(strList[2])+";";
                //KV_insert(strList[1], strList[2]);
                break;
            case 2:
                //query_result = KV_update(strList[1], strList[2]);
                break;
            case 3:
                start = std::chrono::steady_clock::now();
                query_result = "pointsearch;"+fltr_gen.get_filter(strList[1])+";";
                stop = std::chrono::steady_clock::now();
                total_ps1 += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
                num_ps++;
                break;
            case 4:
                start = std::chrono::steady_clock::now();
                query_result = "rangesearch;"+fltr_gen.get_filter(strList[1])+";"+fltr_gen.get_filter(strList[2])+";";
                stop = std::chrono::steady_clock::now();
                total_rs1 = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
                num_rs++;
                break;
            case 5:
                //query_result = KV_drop(strList[1]);
                break;
            default:
                break;
        }
    //cout << "Sub-step: 10" << endl;
    return query_result;
};

string parser2(string query) {
    //cout << "Sub-step: 1" << endl;
    vector<string> sentences;
    //cout << "Sub-step: 2" << endl;
    query.pop_back();
    //cout << "Sub-step: 3" << endl;
    //Stringsplit(query, '\n', sentences);
    //cout << "Sub-step: 4" << endl;
    string query_result = "";
    //cout << "Sub-step: 5" << endl;
    //for (int o = 0; o < sentences.size(); o++) {
        //cout << "Sub-step: 6" << endl;
        vector<string> strList;
        //cout << "Sub-step: 7" << endl;
        Stringsplit(query, ';', strList);
        //cout << "Sub-step: 8" << endl;
        /*
        cout << "\n\n" << endl;
        for(int i = 0; i < strList.size(); i++) {
            cout << "->>>>>receiving>" << strList[i] << "<" << endl;
        }
        */
        //cout << "Sub-step: 9" << endl;
        switch(parser_case[strList[0]]) {
            case 1:
                //query_result = "insert "+fltr_gen.get_filter(strList[1])+" "+my_bgv.encrypt(strList[2])+"\n";
                //KV_insert(strList[1], strList[2]);
                query_result = query;
                break;
            case 2:
                //query_result = KV_update(strList[1], strList[2]);
                query_result = query;
                break;
            case 3:
                start = std::chrono::steady_clock::now();
                query_result = my_bgv.decrypt2(my_bgv.keyswitch(strList[1])) + ";";
                stop = std::chrono::steady_clock::now();
                total_ps2 += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
                break;
            case 4:
                start = std::chrono::steady_clock::now();
                for (int ui = 1; ui < strList.size(); ui++) {
                    query_result = query_result + my_bgv.decrypt2(my_bgv.keyswitch(strList[ui])) + ";";
                }
                stop = std::chrono::steady_clock::now();
                total_rs2 += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
                break;
            case 5:
                //query_result = KV_drop(strList[1]);
                query_result = query;
                break;
            default:
                query_result = query;
                break;
        }
    //cout << "Sub-step: 10" << endl;
    //query_result += "\n";
    return query_result;
};

void getConn()
{
    while(run2)
    {
        int conn = accept(s, (struct sockaddr*)&servaddr, &len);
        li.push_back(conn);
        printf("%d\n", conn);
    }
}

void AssignQuery(char* query)
{
    char exit_info[5];
    exit_info[0] = 'e';
    exit_info[1] = 'x';
    exit_info[2] = 'i';
    exit_info[3] = 't';
    exit_info[4] = ';';
    exit_info[5] = '\0';
    string temp;
    if(strcmp(query, "exit;") == 0) {
        temp = exit_info;
    }
    else {
        temp = parser(query);
    }

    for (int i = 0; i < (li.size()-1); i++) {
        char new_query[1024];
        //cout << "step 7" << endl;
        //cout << "AssignQ1:>" << query << endl;
        //cout << "Afterparser:>" << temp << endl;
        for (size_t i = 0; i < temp.length(); i++) {
            new_query[i] = temp[i];
        }
        new_query[temp.length()] = '\0';
        send(li[i], new_query, strlen(new_query), 0);
        //cout << "send1:>" << new_query << endl;
    }
    return ;
}

void ReturnResult(char* result)
{
    char new_result[1024];
    string temp = parser2(result);
    for (size_t i = 0; i < temp.length(); i++) {
            new_result[i] = temp[i];
        }
    new_result[temp.length()] = '\0';
    send(li[(li.size()-1)], new_result, strlen(new_result), 0);
    //cout << "send2:>" << new_result << endl;
    return ;
}

void getData()
{
    struct timeval tv;
    tv.tv_sec = 10;//设置倒计时时间
    tv.tv_usec = 0;
    while(run)
    {
        for(int i=0; run && i<li.size(); ++i)
        {           
            fd_set rfds;   
            FD_ZERO(&rfds);
            int maxfd = 0;
            int retval = 0;
            FD_SET(li[i], &rfds);
            if(maxfd < li[i])
            {
                maxfd = li[i];
            }
            retval = select(maxfd+1, &rfds, NULL, NULL, &tv);
            if(retval == -1)
            {
                //printf("select error\n");
            }
            else if(retval == 0)
            {
                //printf("not message\n");
            }
            else
            {
                char buf[1024];
                memset(buf, 0 ,sizeof(buf));
                int len = recv(li[i], buf, sizeof(buf), 0);
                //if (0 < sizeof(buf)) cout << "Received from>" << i << "<:"<< buf << endl;
                if (i == (li.size()-1)) { // from client
                    AssignQuery(buf);
                    if(strcmp(buf, "exit;") == 0) {
                        run = false;
                        break;
                    };
                }
                else if (i == (li.size()-2)) { // from server
                    ReturnResult(buf);
                }
            }
        }
        //sleep(1);
    }
    run2 = false;
}
 
int main(int argc, char* argv[])
{
    PORT = stoi(argv[1]);
    //new socket
    s = socket(AF_INET, SOCK_STREAM, 0);
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(PORT);
    servaddr.sin_addr.s_addr = inet_addr(IP);
    if(bind(s, (struct sockaddr* ) &servaddr, sizeof(servaddr))==-1)
    {
        perror("bind");
        exit(1);
    }
    if(listen(s, 20) == -1)
    {
        perror("listen");
        exit(1);
    }
    len = sizeof(servaddr);
 
    //thread : while ==>> accpet
    std::thread t(getConn);
    t.detach();//detach的话后面的线程不同等前面的进程完成后才能进行，如果这里改为join则前面的线程无法判断结束，就会
    //一直等待，导致后面的线程无法进行就无法实现操作
    //printf("done\n");
    //thread : input ==>> send
    //std::thread t1(sendMess);
    //t1.detach();
    //thread : recv ==>> show
    std::thread t2(getData);
    t2.detach();
    while(run2)//做一个死循环使得主线程不会提前退出
    {
 
    }
    cout << (total_ps1+total_ps2) << " / " << double(num_ps) << endl;
    cout << (total_ps1+total_ps2) / double(num_ps) << endl;
    cout << (total_rs1+total_rs2) << " / " << double(num_rs) << endl;
    cout << (total_rs1+total_rs2) / double(num_rs) << endl;
    cout << "filter: " << total_filter << " / "<< double(num_filter) << endl;
    cout << total_filter / double(num_filter) << endl;
    cout << "enc: " << total_enc << " / "<< double(num_enc) << endl;
    cout << total_enc / double(num_enc) << endl;
    cout << "dec: " << total_dec << " / "<< double(num_dec) << endl;
    cout << total_dec / double(num_dec) << endl;
    cout << "ks: " << total_ks << " / "<< double(num_ks) << endl;
    cout << total_ks / double(num_ks) << endl;
    return 0;
}