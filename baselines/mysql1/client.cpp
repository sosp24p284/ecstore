#include <stdio.h>
#include <unistd.h>
#include <iostream>
#include <mysql/mysql.h>

#include <chrono>
#include <cstring>
#include <string.h>
#include <cmath>
#include <vector>
#include <algorithm>

#include <chrono>

#define num_byte1 8
#define num_byte2 64
#define num_hash 3
#define max_rand 10000
#define length_unit_bloom 10

using namespace std;

bool ptxt_mode;
auto start = std::chrono::steady_clock::now();
auto stop = std::chrono::steady_clock::now();
auto total_insert = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
auto total_delete = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
int num_insert = 0;
int num_delete = 0;

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
        int qs = 1018081;
        int ts = 841;
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

void insert(MYSQL &m, string a, string b) {
    string mysql_a = ptxt_mode ? a : fltr_gen.get_filter(a);
    string mysql_b = ptxt_mode ? b : my_bgv.encrypt(b);
    //cout << "insert username: " << mysql_a << endl;
    string sql0 = ptxt_mode ? "insert into usersP(username,password) values(\"" + mysql_a + "\",\"" + mysql_b + "\");" : "insert into usersE(username,password) values(\"" + mysql_a + "\",\"" + mysql_b + "\");";
    const char* sql = sql0.c_str();
    start = std::chrono::steady_clock::now();
    int rc = mysql_real_query(&m, sql, strlen(sql));
    stop = std::chrono::steady_clock::now();
    total_insert += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
    num_insert += 1;
    if (0 != rc) {
        printf("mysql_real_query(): %s\n", mysql_error(&m));
    }
    return ;
};

void pq(MYSQL &m, string a) {
    string mysql_a = ptxt_mode ? a : fltr_gen.get_filter(a);
    string sql0 = ptxt_mode ? "select * from usersP where username=\"" + mysql_a + "\";" : "select * from usersE where username=\"" + mysql_a + "\";";
    const char* sql = sql0.c_str();
    int rc = mysql_real_query(&m, sql, strlen(sql));
    if (0 != rc) {
        printf("mysql_real_query(): %s\n", mysql_error(&m));
    }
    MYSQL_RES* result = mysql_store_result(&m);
	if( !result ) {
		cout << "result error, line : " << __LINE__ << endl;
	}
    return ;
};

void rq(MYSQL &m, string a, string b) {
    string mysql_a = ptxt_mode ? a : fltr_gen.get_filter(a);
    string mysql_b = ptxt_mode ? b : fltr_gen.get_filter(b);
    string sql0 = ptxt_mode ? "select * from usersP where username between \"" + mysql_a + "\" and \"" + mysql_b + "\";" : "select * from usersE where username between \"" + mysql_a + "\" and \"" + mysql_b + "\";";
    const char* sql = sql0.c_str();
    int rc = mysql_real_query(&m, sql, strlen(sql));
    if (0 != rc) {
        printf("mysql_real_query(): %s\n", mysql_error(&m));
    }
    MYSQL_RES* result = mysql_store_result(&m);
	if( !result ) {
		cout << "result error, line : " << __LINE__ << endl;
	}
    return ;
};

void drop(MYSQL &m, string a) {
    string mysql_a = ptxt_mode ? a : fltr_gen.get_filter(a);
    string sql0 = ptxt_mode ? "delete from usersP where username=\"" + mysql_a + "\";" : "delete from usersE where username=\"" + mysql_a + "\";";
    const char* sql = sql0.c_str();
    start = std::chrono::steady_clock::now();
    int rc = mysql_real_query(&m, sql, strlen(sql));
    stop = std::chrono::steady_clock::now();
    total_delete += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
    num_delete += 1;
    if (0 != rc) {
        printf("mysql_real_query(): %s\n", mysql_error(&m));
    }
    return ;
};

void join() {

};

void update() {

};

int main(int argc, char* argv[])
{
    int num_entry = pow(2, stoi(argv[1]));
    int ptxt = stoi(argv[2]);
    int rc;
    ptxt_mode = ptxt == 1 ? true : false;

    MYSQL mysql;
    //1.初始化操作句柄
    //MYSQL *     STDCALL mysql_init(MYSQL *mysql);
    mysql_init(&mysql);
    /*
     * 操作句柄
     * host ： 服务端所在机器的ip地址
     * 数据库的用户名：
     * 数据库的密码：
     * 数据库名称
     * 数据库端口
     * unix_socket : AF_UNIX（进程间通信的方式）  AF_INET（网络套接字）
     * clientflag : CLIENT_FOUND_ROWS
     * */
    if(!mysql_real_connect(&mysql,"localhost", "root", "letmein", "test", 6000, NULL, CLIENT_FOUND_ROWS))
    {
        //const char * STDCALL mysql_error(MYSQL *mysql);
        cout << mysql_error(&mysql) << endl;
        return -1;
    }

    // create table
    const char* sql_create = ptxt_mode ? "create table usersP(id int(10) not null primary key auto_increment,username varchar(10),password varchar(100));" : "create table usersE(id int(10) not null primary key auto_increment,username varchar(100),password varchar(512));";
    rc = mysql_real_query(&mysql, sql_create, strlen(sql_create));
    if (0 != rc) {
        printf("mysql_real_query(): %s\n", mysql_error(&mysql));
    }

    // insert
    char* username = new char[num_byte1];
    char* password = new char[num_byte2];
    for (int i = 0; i < num_entry; i++) {
        for (int o = 0; o < num_byte1; o++) {
            username[o] = static_cast<char>(97+(int(i/pow(26, (num_byte1-o-1))) % 26));
        }
        for (int o = 0; o < num_byte2; o++) {
            password[o] = static_cast<char>(97+(int(i/pow(26, (num_byte2-o-1))) % 26));
        }
        username[num_byte1] = '\0';
        password[num_byte2] = '\0';
        insert(mysql, username, password);
    }

    // point query

    // range query

    // join

    // update

    // delete
    for (int i = 0; i < num_entry; i++) {
        for (int o = 0; o < num_byte1; o++) {
            username[o] = static_cast<char>(97+(int(i/pow(26, (num_byte1-o-1))) % 26));
        }
        username[num_byte1] = '\0';
        drop(mysql, username);
    }

    /*
    const char* sql = "insert into users(username,password) values(\"user1\",\"111111\");";
    cout << "The query is: " << endl;
    mysql_query(&mysql, sql);
    */
    
    const char* sql_drop = ptxt_mode ? "drop table usersP;" : "drop table usersE;";
    rc = mysql_real_query(&mysql, sql_drop, strlen(sql_drop));
    if (0 != rc) {
        printf("mysql_real_query(): %s\n", mysql_error(&mysql));
    }
    mysql_close(&mysql);

    // result
    if (ptxt_mode) {
        cout << "Plaintext result->>" << endl;
    }
    else {
        cout << "HoTEE result->>" << endl;
    };
    cout << "Insert: " << total_insert / double(num_insert) / double(1000000) << " Tput: " << 1000 / (total_insert / double(num_insert) / double(1000000)) << endl;
    cout << "Delete: " << total_delete / double(num_delete) / double(1000000) << " Tput: " << 1000 / (total_delete / double(num_delete) / double(1000000)) << endl;
    cout << "Completed." << endl;
    
    return 0;
}