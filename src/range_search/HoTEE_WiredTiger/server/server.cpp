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

#define PORT 7001
#define QUEUE 20
#define BUFFSIZE 1024
#define SENDBACK_SIZE 2048

#include <cstdlib>
#include <cstring>
#include <cmath>
#include <vector>
#include <map>
//#include <iostream>
#include <chrono>
#include <algorithm>
#include <wiredtiger.h>

#include <cmath>

#include <thread>
#include <queue>

#define length_unit_bloom 10
#define num_hash 3
#define max_rand 10000
#define k_byte 8
#define v_byte 64
#define qs 1018081
#define ts 841

using namespace std;

int num_insert = 0;
int conn0;
WT_CONNECTION *conn;
WT_SESSION *session;
WT_CURSOR *cursor;
static const char *home;
int ret;
queue<string> query_que;
queue<string> result_que;
bool execute = true;

map<string, int>parser_case = {
    {"insert", 1},
    {"update", 2},
    {"pointsearch", 3},
    {"rangesearch", 4},
    {"delete", 5},
    {"chec", 6},
    {"exit", 7}
};

char* mystr(string x) {
    char* new_char = new char[64];
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
            return result;
        };
        string decrypt1(string ctxt) {
            string result;
            int num = ctxt.length() / 8;
            for (int i = 0; i < num; i++) {
                string temp_b = ctxt.substr(8*i, 4);
                string temp_a0 = ctxt.substr(8*i+4, 4);
                result.push_back(static_cast<char>(97 + ((ts+qs+atoi(temp_b.c_str()) - atoi(temp_a0.c_str()) * s)% q % t)));
            }
            return result;
        };
        string decrypt2(string ctxt) {
            string result;
            int num = ctxt.length() / 8;
            for (int i = 0; i < num; i++) {
                string temp_b = ctxt.substr(8*i, 4);
                string temp_a0 = ctxt.substr(8*i+4, 4);
                result.push_back(static_cast<char>(97 + ((qs+ts+atoi(temp_b.c_str()) - atoi(temp_a0.c_str()) * s2) % q % t)));
            }
            return result;
        };
        string keyswitch(string ctxt) {
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
            return result;
        };
};

bgv my_bgv;

// 使用字符分割
void Stringsplit(const string& str, const char split, queue<string>& res)
{
    if (str == "")        return;
    //在字符串末尾也加入分隔符，方便截取最后一段
    string strs = str + split;
    size_t pos = strs.find(split);
 
    // 若找不到内容则字符串搜索函数返回 npos
    while (pos != strs.npos)
    {
        string temp = strs.substr(0, pos);
        if (0 < temp.length()) {
            res.push(temp);
            }
        //去掉已分割的字符串,在剩下的字符串中进行分割
        strs = strs.substr(pos + 1, strs.size());
        pos = strs.find(split);
    }
};

string KV_pointSearch(string k) {
    //string temp_key = mystr(k);
    //cout << "Key sent back: " << temp_key << endl;
    const char *key = mystr(fltr_gen.get_filter(k));//temp_key.c_str();
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
    string temp1 = fltr_gen.get_filter(k);
    string temp2 = my_bgv.encrypt(v);
    const char *key = temp1.c_str();//temp_key.c_str();
    const char *value = temp2.c_str();//v.c_str();
    //cout << "My insert key is: " << key << endl;
    //cout << "My insert value is: " << value << endl;
    cursor->set_key(cursor, key);
    cursor->set_value(cursor, value);
    cursor->insert(cursor);
    char* temp = "Success insert";
    string result = temp;
    num_insert++;
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
    const char *key = mystr(fltr_gen.get_filter(k1));//temp_key.c_str();
    cursor->set_key(cursor, key);
    if (cursor->search(cursor) != 0) {
        result = "NotFound";
        return result;
    }
    const char* result_temp;
    cursor->get_value(cursor, &result_temp);
    string result0 = result_temp;
    result = my_bgv.decrypt1(result0);
    //string temp_key2 = fltr_gen.get_filter(k2);
    //cout << "Key sent back2: " << temp_key2 << endl;
    const char *key2 = mystr(fltr_gen.get_filter(k2));//temp_key2.c_str();
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
            string temp3 = my_bgv.decrypt1(result_temp2);
            result += temp3;
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
    queue<string> sentences;
    query.pop_back();
    Stringsplit(query, '\n', sentences);
    string query_result;
    for (int o = 0; o < sentences.size(); o++) {
        queue<string> strList;
        Stringsplit(sentences.front(), ' ', strList);
        /*
        cout << "\n\n" << endl;
        for(int i = 0; i < strList.size(); i++) {
            cout << "->>>>>receiving>" << strList[i] << "<" << endl;
        }
        */
       /*
       string temp1 = strList.front();
        switch(parser_case[temp1]) {
            case 1:
                query_result = KV_insert(strList[1], strList[2]);
                break;
            case 2:
                query_result = KV_update(strList[1], strList[2]);
                break;
            case 3:
                query_result = KV_pointSearch(strList[1]);
                break;
            case 4:
                query_result = KV_rangeSearch(strList[1], strList[2]);
                break;
            case 5:
                query_result = KV_drop(strList[1]);
                break;
            default:
                break;
        }
        */
    }
    return query_result;
};

void send_result() {
    //cout << "start send result" << endl;
    while(execute) {
        if (!result_que.empty()) {
            string result = result_que.front();
            result_que.pop();
            char query_result[1024];
            for (size_t i = 0; i < result.length(); i++) {
                query_result[i] = result[i];
            }
            query_result[result.length()] = '\0';
            send(conn0, query_result, sizeof(query_result), 0);
        }
    }
}

void execute_query() {
    auto start = std::chrono::steady_clock::now();
    auto stop = std::chrono::steady_clock::now();
    auto total_ps = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
    auto total_rs = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
    auto interval = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
    int num_ps = 0;
    int num_rs = 0;
    //cout << "start execute query\n" << endl;
    char check_info[5];
    check_info[0] = 'c';
    check_info[1] = 'h';
    check_info[2] = 'e';
    check_info[3] = 'c';
    check_info[4] = ';';
    check_info[5] = '\0';
    char exit_info[5];
    exit_info[0] = 'e';
    exit_info[1] = 'x';
    exit_info[2] = 'i';
    exit_info[3] = 't';
    exit_info[4] = ';';
    exit_info[5] = '\0';
    queue<string> sentences;
    while(execute) {
        if (!query_que.empty()) {
            string temp = query_que.front();
            query_que.pop();
            if (temp == "") continue;
            Stringsplit(temp, ';', sentences);
            bool oo = true;
            while (oo && !sentences.empty()) {
                string query_result = "";
                string temp1 = sentences.front();
                switch(parser_case[temp1]) {
                    case 1:
                        if (2 < sentences.size()) {
                            sentences.pop();
                            string temp2 = sentences.front();
                            sentences.pop();
                            string temp3 = sentences.front();
                            sentences.pop();
                            //cout << "insert*" << temp2 << "*" << temp3 << endl;
                            query_result = KV_insert(temp2, temp3);
                        }
                        else {
                            oo = false;
                        };
                        break;
                    case 2:
                        if (2 < sentences.size()) {
                            //cout << "update*" << sentences[o+1] << sentences[o+2] << endl;
                            sentences.pop();
                            string temp2 = sentences.front();
                            sentences.pop();
                            string temp3 = sentences.front();
                            sentences.pop();
                            //cout << "update*" << temp2 << temp3 << endl;
                            query_result = KV_update(temp2, temp3);
                        }
                        else {
                            oo = false;
                        }
                        break;
                    case 3:
                        if (1 < sentences.size()) {
                            //cout << "pointSearch*" << sentences[o+1] << endl;
                            sentences.pop();
                            string temp2 = sentences.front();
                            sentences.pop();
                            //cout << "pointSearch*" << temp2 << endl;
                            start = std::chrono::steady_clock::now();
                            query_result = KV_pointSearch(temp2);
                            stop = std::chrono::steady_clock::now();
                            total_ps += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
                            num_ps++;
                        }
                        else {
                            oo = false;
                        }
                        break;
                    case 4:
                        if (2 < sentences.size()) {
                            //cout << "rangeSearch*" << sentences[o+1] << sentences[o+2] << endl;
                            sentences.pop();
                            string temp2 = sentences.front();
                            sentences.pop();
                            string temp3 = sentences.front();
                            sentences.pop();
                            //cout << "rangeSearch*" << temp2 << " * " << temp3 << endl;
                            start = std::chrono::steady_clock::now();
                            query_result = KV_rangeSearch(temp2, temp3);
                            stop = std::chrono::steady_clock::now();
                            interval = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
                            total_rs += interval;
                            cout << interval << "," << endl;
                            num_rs++;
                        }
                        else {
                            oo = false;
                        };
                        break;
                    case 5:
                        if (1 < sentences.size()) {
                            //cout << "drop*" << sentences[o+1] << endl;
                            sentences.pop();
                            string temp2 = sentences.front();
                            sentences.pop();
                            //cout << "drop*" << temp2 << endl;
                            query_result = KV_drop(temp2);
                        }
                        else {
                            oo = false;
                        }
                        break;
                    case 6:
                        //cout << "check*" << endl;
                        sentences.pop();
                        query_result = check_info;
                        break;
                    case 7:
                        //cout << "exit*" << endl;
                        sentences.pop();
                        //query_result = exit_info;
                        execute = false;
                        break;
                    default:
                        //cout << "default*" << endl;
                        break;
                };
                if (query_result != "") {
                    //cout << "result:" << query_result << "<"<< endl;
                    result_que.push(query_result);
                }
            };
        };
    };
    //cout << total_ps << " / " << num_ps << endl;
    cout << log(num_insert)/log(2) << endl;
    cout << total_ps / double(num_ps) << endl;
    //cout << total_rs << " / " << num_rs << endl;
    cout << total_rs / double(num_rs) << endl;
};

queue<string> parser1_que;

void parser1() {
    string left = "";
    while(1) {
        if (!parser1_que.empty()) {
            string temp = parser1_que.front();
            temp = left + temp;
            parser1_que.pop();
            int i = temp.length() - 1;
            for (; 0 <= i; i--) {
                if (temp[i] == ';') {
                    break;
                }
            };
            if (i < 0) {
                left = temp;
            }
            else {
                left = temp.substr((i+1), temp.length());
            };
            temp = temp.substr(0, i+1);
            //cout << "parser1*" << temp << "\n" << endl;
            query_que.push(temp);
        }
    };
};

int main()
{
    cout << "\n>---------" << endl;
    //打开一个wiredtiger要操作的db目录
    if (getenv("WIREDTIGER_HOME") == NULL) {
        home = "WT_HOME";
        ret = system("rm -rf WT_HOME && mkdir WT_HOME");
    } else {
        home = NULL;
    }

    wiredtiger_open(home, NULL, "create,cache_size=4GB", &conn);
    conn->open_session(conn, NULL, NULL, &session);
    session->create(session, "table:access", "key_format=S,value_format=S");
    session->open_cursor(session, "table:access", NULL, NULL, &cursor);

    char check_info[5];
    check_info[0] = 'c';
    check_info[1] = 'h';
    check_info[2] = 'e';
    check_info[3] = 'c';
    check_info[4] = ';';
    check_info[5] = '\0';

    fd_set rfds;
    struct timeval tv;
    int retval, maxfd;
    int ss = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in server_sockaddr;
    server_sockaddr.sin_family = AF_INET;
    server_sockaddr.sin_port = htons(PORT);
    //printf("%d\n",INADDR_ANY);
    server_sockaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    if(bind(ss, (struct sockaddr* ) &server_sockaddr, sizeof(server_sockaddr))==-1)
    {
        perror("bind");
        exit(1);
    }
    if(listen(ss, QUEUE) == -1)
    {
        perror("listen");
        exit(1);
    }
 
    struct sockaddr_in client_addr;
    socklen_t length = sizeof(client_addr);
    ///成功返回非负描述字，出错返回-1
    conn0 = accept(ss, (struct sockaddr*)&client_addr, &length);
    /*没有用来存储accpet返回的套接字的数组，所以只能实现server和单个client双向通信*/
    if( conn < 0 )
    {
        perror("connect");
        exit(1);
    }

    std::thread t1(send_result);
    std::thread t2(execute_query);
    std::thread t3(parser1);
    t1.detach();
    t2.detach();
    t3.detach();
    string left = "";
    while(execute)
    {
        /*把可读文件描述符的集合清空*/
        FD_ZERO(&rfds);
        /*把标准输入的文件描述符加入到集合中*/
        FD_SET(0, &rfds);
        maxfd = 0;
        /*把当前连接的文件描述符加入到集合中*/
        FD_SET(conn0, &rfds);
        /*找出文件描述符集合中最大的文件描述符*/   
        if(maxfd < conn0)
            maxfd = conn0;
        /*设置超时时间*/
        tv.tv_sec = 5;//设置倒计时
        tv.tv_usec = 0;
        /*等待聊天*/
        retval = select(maxfd+1, &rfds, NULL, NULL, &tv);
        if(retval == -1)
        {
            printf("select出错, 客户端程序退出\n");
            break;
        }
        else if(retval == 0)
        {
            //printf("[5s]");
            continue;
        }
        else
        {
            /*客户端发来了消息*/
            if(FD_ISSET(conn0,&rfds))
            {
                char buffer[1024];   
                memset(buffer, 0 ,sizeof(buffer));
                int len = recv(conn0, buffer, sizeof(buffer), 0);
                //cout << "*" << buffer << "*\n"<< endl;
                //if(strcmp(buffer, "exit;") == 0) break;
                //if(strcmp(buffer, "chec;") == 0) {
                //    cout << "receive chec" << endl;
                //    send(conn0, check_info, sizeof(check_info), 0);
                //};
                string temp = buffer;
                if (1024 <= temp.length()) {
                    temp.pop_back();
                    temp.pop_back();
                    temp.pop_back();
                }
                parser1_que.push(temp);
                //query_que.push(temp);
            }
            /*用户输入信息了,开始处理信息并发送*/
            /*
            if(FD_ISSET(0, &rfds))
            {
                char buf[1024];
                fgets(buf, sizeof(buf), stdin);
                //printf("you are send %s", buf);
                send(conn, buf, sizeof(buf), 0);
            }
            */
        }
    }
    execute = false;
    close(conn0);
    close(ss);
    cout << "----------<\n" << endl;
    return 0;
}