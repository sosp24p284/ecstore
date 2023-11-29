#include <stdio.h>
#include <unistd.h>
#include <iostream>
#include <mysql/mysql.h>
using namespace std;

int main()
{
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

    const char* sql = "insert into users(username,password) values(\"user1\",\"111111\");";
    cout << "The query is: " << endl;
    mysql_query(&mysql, sql);
    mysql_close(&mysql);
    cout << "Succeed!" << endl;
    return 0;
}