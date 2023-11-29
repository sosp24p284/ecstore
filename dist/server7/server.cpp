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

//#define MYPORT  7000
#define BUFFER_SIZE 1024
#define BUFFSIZE 1024
#define length_unit_bloom 10
#define num_hash 3
#define max_rand 10000
#define k_byte 8
#define v_byte 64

using namespace std;

int MYPORT;

WT_CONNECTION *conn;
WT_SESSION *session;
WT_CURSOR *cursor;
static const char *home;
int ret;

auto start = std::chrono::steady_clock::now();
auto stop = std::chrono::steady_clock::now();
auto total_ps = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
auto total_rs = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
int num_ps = 0;
int num_rs = 0;

map<string, int>parser_case = {
    {"insert", 1},
    {"update", 2},
    {"pointsearch", 3},
    {"rangesearch", 4},
    {"delete", 5}
};

char* mystr(string x) {
    char* new_char = new char[1024];
    x.copy(new_char, x.length(), 0);
    *(new_char+x.length())='\0';
    return new_char;
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
            int x_num = 0;
            for (const auto & ch : x) {
                x_num = x_num * 26 + ch;
            };
            //cout << "The serialized number is: " << x_num << endl;
            return get_filter(x_num);
        };
};

filter_generator fltr_gen(1);

// 使用字符分割
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

string KV_pointSearch(string k) {
    //string temp_key = mystr(k);
    //cout << "Key sent back: " << temp_key << endl;
    const char *key = mystr(k);//temp_key.c_str();
    //cout << "1" << endl;
    cursor->set_key(cursor, key);
    //cout << "2" << endl;
    const char* result_temp;
    string result;
    if (cursor->search(cursor) == 0) {
        cursor->get_value(cursor, &result_temp);
        result = result_temp;
    }
    else {
        result = "NotFound";
    }
    //cout << "3" << endl;
    //cout << "4: " << result_temp << endl;
    //cursor->reset(cursor);
    return result;
};


string KV_insert(string k, string v) {
    //string temp_key = fltr_gen.get_filter(k);
    //cout << "Key sent back: " << temp_key << endl;
    
    const char *key = mystr(k);//temp_key.c_str();
    const char *value = mystr(v);//v.c_str();
    //cout << "My insert key is: " << key << endl;
    //cout << "My insert value is: " << value << endl;
    cursor->set_key(cursor, key);
    cursor->set_value(cursor, value);
    cursor->insert(cursor);
    char* temp = "Success insert";
    //cout << temp << endl;
    string result = temp;
    return result;
};

string KV_update(string k, string v) {
    string result;
    char* temp = "UPDATE";
    result = temp;
    cout << "update!!" << endl;
    return result;
};

string KV_rangeSearch(string k1, string k2) {
    string result;
    //string temp_key = fltr_gen.get_filter(k1);
    //cout << "Key sent back1: " << temp_key << endl;
    const char *key = mystr(k1);//temp_key.c_str();
    cursor->set_key(cursor, key);
    if (cursor->search(cursor) != 0) {
        result = "NotFound";
        return result;
    }
    const char* result_temp;
    cursor->get_value(cursor, &result_temp);
    result = result_temp;
    //string temp_key2 = fltr_gen.get_filter(k2);
    //cout << "Key sent back2: " << temp_key2 << endl;
    const char *key2 = mystr(k2);//temp_key2.c_str();
    while ((ret = cursor->next(cursor)) == 0) {
        const char* result_temp2;
        const char* key_temp;
        cursor->get_key(cursor, &key_temp);
        if (0 < strcmp(key_temp, key2)) {
            break;
        };
        cursor->get_value(cursor, &result_temp2);
        if (result.length() < BUFFSIZE) {
            result += ";";
            result += result_temp2;
        };
    }
    //cursor->reset(cursor);
    return result;
};

string KV_drop(string k) {
    string result;
    char* temp = "DELETE";
    result = temp;
    cout << "delete!!" << endl;
    return result;
};

string parser(string query) {
    //cout << "Sub-step: 1" << endl;
    vector<string> sentences;
    //cout << "Sub-step: 2" << endl;
    query.pop_back();
    //cout << "Sub-step: 3" << endl;
    Stringsplit(query, '\n', sentences);
    //cout << "Sub-step: 4" << endl;
    string query_result;
    //cout << "Sub-step: 5" << endl;
    for (int o = 0; o < sentences.size(); o++) {
        //cout << "Sub-step: 6" << endl;
        vector<string> strList;
        //cout << "Sub-step: 7" << endl;
        Stringsplit(query, ';', strList);
        //cout << "Sub-step: 8" << endl;
        /*
        cout << "\n\n" << endl;
        for(int i = 0; i < strList.size(); i++) {
            cout << "->>>>>parser receiving>\n" << strList[i] << "<" << endl;
        }
        */
        //cout << "Sub-step: 9" << endl;
        switch(parser_case[strList[0]]) {
            case 1:
                query_result = KV_insert(strList[1], strList[2]);
                break;
            case 2:
                query_result = KV_update(strList[1], strList[2]);
                break;
            case 3:
                start = std::chrono::steady_clock::now();
                query_result = "pointsearch;"+KV_pointSearch(strList[1]);
                stop = std::chrono::steady_clock::now();
                total_ps += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
                num_ps++;
                break;
            case 4:
                start = std::chrono::steady_clock::now();
                query_result = "rangesearch;"+KV_rangeSearch(strList[1], strList[2]);
                stop = std::chrono::steady_clock::now();
                total_rs += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
                num_rs++;
                break;
            case 5:
                query_result = KV_drop(strList[1]);
                break;
            default:
                break;
        }
    }
    //cout << "Sub-step: 10" << endl;
    query_result += ";";
    return query_result;
};

int main(int argc, char* argv[])
{
    MYPORT = stoi(argv[1]);
    
    //打开一个wiredtiger要操作的db目录
    if (getenv("WIREDTIGER_HOME") == NULL) {
        home = "WT_HOME7";
        ret = system("rm -rf WT_HOME7 && mkdir WT_HOME7");
    } else {
        home = NULL;
    }

    wiredtiger_open(home, NULL, "create,cache_size=4GB", &conn);
    conn->open_session(conn, NULL, NULL, &session);
    session->create(session, "table:access", "key_format=S,value_format=S");
    session->open_cursor(session, "table:access", NULL, NULL, &cursor);


    int sock_cli;
    fd_set rfds;
    struct timeval tv;
    int retval, maxfd;
 
    ///定义sockfd
    sock_cli = socket(AF_INET,SOCK_STREAM, 0);
    ///定义sockaddr_in
    struct sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(MYPORT);  ///服务器端口
    servaddr.sin_addr.s_addr = inet_addr("127.0.0.1");  ///服务器ip
 
    //连接服务器，成功返回0，错误返回-1
    if (connect(sock_cli, (struct sockaddr *)&servaddr, sizeof(servaddr)) < 0)
    {
        perror("connect");
        exit(1);
    }
 
    while(1){
        /*把可读文件描述符的集合清空*/
        FD_ZERO(&rfds);
        /*把标准输入的文件描述符加入到集合中*/
        FD_SET(0, &rfds);
        maxfd = 0;
        /*把当前连接的文件描述符加入到集合中*/
        FD_SET(sock_cli, &rfds);
        /*找出文件描述符集合中最大的文件描述符*/   
        if(maxfd < sock_cli)
            maxfd = sock_cli;
        /*设置超时时间*/
        tv.tv_sec = 10;
        tv.tv_usec = 0;
        /*等待聊天*/
        retval = select(maxfd+1, &rfds, NULL, NULL, &tv);
        if(retval == -1){
            //printf("select出错，客户端程序退出\n");
            break;
        }else if(retval == 0){
            //printf("waiting...\n");
            continue;
        }else{
            /*服务器发来了消息*/
            if(FD_ISSET(sock_cli,&rfds)){
                //cout << "step 1" << endl;
                char recvbuf[BUFFER_SIZE];
                //cout << "step 2" << endl;
                memset(recvbuf, 0, sizeof(recvbuf));
                //cout << "step 3" << endl;
                int len;
                //cout << "step 4" << endl;
                len = recv(sock_cli, recvbuf, sizeof(recvbuf),0);
                //cout << "step 5" << endl;
                //printf("%s", recvbuf);
                if(strcmp(recvbuf, "exit;") == 0) break;
                //cout << "receiving:>" << recvbuf << endl;
                string temp = parser(recvbuf);
                //cout << "step 6" << endl;
                char query_result[1024];
                //cout << "step 7" << endl;
                for (size_t i = 0; i < temp.length(); i++)
                {
                    query_result[i] = temp[i];
                }
                //cout << "step 8" << endl;
                query_result[temp.length()] = '\0';
                //cout << "step 9" << endl;
                send(sock_cli, query_result, sizeof(query_result), 0);
                //cout << "step 10" << endl;
            }
            /*用户输入信息了,开始处理信息并发送*/
            if(FD_ISSET(0, &rfds)){
                char sendbuf[BUFFER_SIZE];
                fgets(sendbuf, sizeof(sendbuf), stdin);
                send(sock_cli, sendbuf, strlen(sendbuf),0); //发送
                memset(sendbuf, 0, sizeof(sendbuf));
            }
        }
    }
    cout << total_ps / double(num_ps) << endl;
    cout << total_rs / double(num_rs) << endl;
    close(sock_cli);
    return 0;
}