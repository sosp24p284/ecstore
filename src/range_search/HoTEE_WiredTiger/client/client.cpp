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

#include <cstdlib>
#include <cstring>
#include <cmath>
#include <vector>
#include <map>
#include <iostream>
#include <chrono>
#include <algorithm>

#include <thread>

#define MYPORT  7001
#define BUFFER_SIZE 1024
#define RECEIVE_SIZE 2048
#define k_byte 8
#define v_byte 64
#define num_warmup 10000
#define num_test 1000
#define none_key_ratio 5
#define new_test_num 10000

using namespace std;

int bound = 0;
int num;

int* range_gen() {
    int* result = new int[2*k_byte];
    int a = rand() % num;
    int b = rand() % num;
    if (b < a) {
        int temp = a;
        a = b;
        b = temp;
    }
    for (int i = 0; i < 2*k_byte; i++) {
        result[i] = rand() % 26;
    }
    for (int i = 0; i < k_byte; i++) {
        result[k_byte-1-i] = a % 26;
        result[2*k_byte-1-i] = b % 26;
        a = int(a/26);
        b = int(b/26);
    }
    /*
    if (bound <= a || bound <= b) {
        return range_gen();
    }
    */
    return result;
}

void receive_result(int sock_cli) {
    //cout << "Enter result receive..." << endl;
    auto start_thread = std::chrono::steady_clock::now();
    while(1)
    {
        char recvbuf[BUFFER_SIZE];
        int len;
        len = recv(sock_cli, recvbuf, sizeof(recvbuf),0);
        //if (1 < sizeof(recvbuf)) cout << recvbuf << endl;
        if(strcmp(recvbuf, "chec;exit;") == 0) {
            //cout << "yes" << endl;
            break;
        };
        memset(recvbuf, 0, sizeof(recvbuf));
    }
    auto stop_thread = std::chrono::steady_clock::now();
    auto total_thread = std::chrono::duration_cast<std::chrono::nanoseconds>(stop_thread - start_thread).count();
    cout << total_thread << ", " << endl;
};

int main(int argc, char* argv[])
{
    int num_kv_pair = stoi(argv[1]);
    //cout << "-----> " << num_kv_pair << endl;
    num = pow(2, num_kv_pair);
    bound = int(num * 10 / none_key_ratio);
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

    //Insert
    FD_ZERO(&rfds);
    FD_SET(0, &rfds);
    maxfd = 0;
    FD_SET(sock_cli, &rfds);
    if(maxfd < sock_cli) maxfd = sock_cli;
    int len_insert_query = 6+2+k_byte+v_byte+1;
    char insert_query[len_insert_query];
    insert_query[0] = 'i';
    insert_query[1] = 'n';
    insert_query[2] = 's';
    insert_query[3] = 'e';
    insert_query[4] = 'r';
    insert_query[5] = 't';
    insert_query[6] = ';';
    for (int i = 8+k_byte; i < len_insert_query; i++) {
        insert_query[i] = static_cast<char>(97);
    }
    insert_query[6+2+k_byte+v_byte] = ';';
    insert_query[6+2+k_byte+v_byte+1] = '\0';
    for (int i = 0; i < num; i++) {
        for(int o = 7; o < (7+k_byte); o++) {
            insert_query[o] = static_cast<char>(97+(int(i/pow(26, (14-o))) % 26));
        }
        insert_query[7+k_byte] = ';';
        insert_query[6+2+k_byte+v_byte-1] = static_cast<char>(97+(i%26));
        //cout << "1->:" << insert_query << endl;
        //cout << "[" << i << "]\n" << insert_query << endl;
        send(sock_cli, insert_query, strlen(insert_query), 0);
        //char recvbuf[BUFFER_SIZE];
        //int len;
        //len = recv(sock_cli, recvbuf, sizeof(recvbuf),0);
        //memset(recvbuf, 0, sizeof(recvbuf));
    }

    
    //pointSearch
    int len_ps_query = 11+1+k_byte+1;
    char pq_query[len_ps_query];
    pq_query[0] = 'p';
    pq_query[1] = 'o';
    pq_query[2] = 'i';
    pq_query[3] = 'n';
    pq_query[4] = 't';
    pq_query[5] = 's';
    pq_query[6] = 'e';
    pq_query[7] = 'a';
    pq_query[8] = 'r';
    pq_query[9] = 'c';
    pq_query[10] = 'h';
    pq_query[11] = ';';
    pq_query[12] = 'a';
    pq_query[13] = 'a';
    pq_query[14] = 'a';
    pq_query[15] = 'a';
    pq_query[16] = 'a';
    pq_query[17] = 'a';
    pq_query[18] = 'a';
    pq_query[19] = 'a';
    pq_query[11+1+k_byte] = ';';
    pq_query[11+1+k_byte+1] = '\0';
    char pq_query1[len_ps_query];
    pq_query1[0] = 'p';
    pq_query1[1] = 'o';
    pq_query1[2] = 'i';
    pq_query1[3] = 'n';
    pq_query1[4] = 't';
    pq_query1[5] = 's';
    pq_query1[6] = 'e';
    pq_query1[7] = 'a';
    pq_query1[8] = 'r';
    pq_query1[9] = 'c';
    pq_query1[10] = 'h';
    pq_query1[11] = ';';
    pq_query1[12] = 'a';
    pq_query1[13] = 'a';
    pq_query1[14] = 'a';
    pq_query1[15] = 'a';
    pq_query1[16] = 'a';
    pq_query1[17] = 'a';
    pq_query1[18] = 'a';
    pq_query1[19] = 'b';
    pq_query1[11+1+k_byte] = ';';
    pq_query1[11+1+k_byte+1] = '\0';
    char pq_query2[len_ps_query];
    pq_query2[0] = 'p';
    pq_query2[1] = 'o';
    pq_query2[2] = 'i';
    pq_query2[3] = 'n';
    pq_query2[4] = 't';
    pq_query2[5] = 's';
    pq_query2[6] = 'e';
    pq_query2[7] = 'a';
    pq_query2[8] = 'r';
    pq_query2[9] = 'c';
    pq_query2[10] = 'h';
    pq_query2[11] = ';';
    pq_query2[12] = 'a';
    pq_query2[13] = 'a';
    pq_query2[14] = 'a';
    pq_query2[15] = 'a';
    pq_query2[16] = 'a';
    pq_query2[17] = 'a';
    pq_query2[18] = 'a';
    pq_query2[19] = 'c';
    pq_query2[11+1+k_byte] = ';';
    pq_query2[11+1+k_byte+1] = '\0';
    //cpu warmup
    for (int i = 0; i < num_warmup; i++) {
    //for (int i = 0; i < 2; i++) {
        
        //for(int o = 12; o < (12+k_byte); o++) {
        //    pq_query[o] = static_cast<char>(97+(int(i/pow(26, (19-o))) % 26));
        //}
        
        //cout << "[" << i << "] pqw\n" << pq_query << endl;
        send(sock_cli, pq_query, strlen(pq_query), 0);
        //char recvbuf[BUFFER_SIZE];
        //int len;
        //len = recv(sock_cli, recvbuf, sizeof(recvbuf),0);
        //memset(recvbuf, 0, sizeof(recvbuf));
    }
    //cout << "Warmup completed." << endl;
    //test ps
    auto start = std::chrono::steady_clock::now();
    auto stop = std::chrono::steady_clock::now();
    auto total = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
    int num_query = int(num * 10 / none_key_ratio);
    int start_point = int(num_query / 2) - new_test_num / 2;
    int stop_point = int(num_query / 2) + new_test_num / 2;
    start_point = start_point < 0 ? 0 : start_point;
    int record = 0;
    start = std::chrono::steady_clock::now();
    
    char check_info[5];
    check_info[0] = 'c';
    check_info[1] = 'h';
    check_info[2] = 'e';
    check_info[3] = 'c';
    check_info[4] = ';';
    check_info[5] = '\0';
    
    cout << num_kv_pair << ", ";
    std::thread t1(receive_result, sock_cli);
    t1.detach();
    while(record < new_test_num) {
        for (int i = start_point; i < stop_point && record < new_test_num; i++, record++) {
            //
            //for(int o = 12; o < (12+k_byte); o++) {
            //    pq_query[o] = static_cast<char>(97+(int(i/pow(26, (19-o))) % 26));
            //}
            //
            //cout << "[" << i << "] pqt\n" << pq_query << endl;
            send(sock_cli, pq_query, strlen(pq_query), 0);
            send(sock_cli, pq_query1, strlen(pq_query1), 0);
            send(sock_cli, pq_query2, strlen(pq_query2), 0);
            //char recvbuf[BUFFER_SIZE];
            //int len;
            //len = recv(sock_cli, recvbuf, sizeof(recvbuf),0);
            //memset(recvbuf, 0, sizeof(recvbuf));
        }
    }
    //char recvbuf[BUFFER_SIZE];
    //int len;
    //len = recv(sock_cli, recvbuf, sizeof(recvbuf),0);
    //cout << recvbuf << endl;
    //memset(recvbuf, 0, sizeof(recvbuf));
    send(sock_cli, check_info, strlen(check_info), 0);
    stop = std::chrono::steady_clock::now();
    total += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
    //cout << "Point Search: " << total << " nano seconds" << endl;
    //cout << "# of Op.: " << new_test_num << endl;
    //cout << total << ", ";

    //rangeSearch
    int len_rg_query = 11+1+k_byte+1+k_byte+1;
    char rq_query[len_ps_query];
    rq_query[0] = 'r';
    rq_query[1] = 'a';
    rq_query[2] = 'n';
    rq_query[3] = 'g';
    rq_query[4] = 'e';
    rq_query[5] = 's';
    rq_query[6] = 'e';
    rq_query[7] = 'a';
    rq_query[8] = 'r';
    rq_query[9] = 'c';
    rq_query[10] = 'h';
    rq_query[11] = ';';
    /*
    rq_query[12] = 'a';
    rq_query[13] = 'a';
    rq_query[14] = 'a';
    rq_query[15] = 'a';
    rq_query[16] = 'a';
    rq_query[17] = 'a';
    rq_query[18] = 'a';
    rq_query[19] = 'a';
    rq_query[20] = ';';
    rq_query[21] = 'a';
    rq_query[22] = 'a';
    rq_query[23] = 'a';
    rq_query[24] = 'a';
    rq_query[25] = 'a';
    rq_query[26] = 'a';
    rq_query[27] = 'a';
    rq_query[28] = 'e';
    */
    rq_query[len_rg_query-1] = ';';
    rq_query[len_rg_query] = '\0';
    char rq_query1[len_ps_query];
    rq_query1[0] = 'r';
    rq_query1[1] = 'a';
    rq_query1[2] = 'n';
    rq_query1[3] = 'g';
    rq_query1[4] = 'e';
    rq_query1[5] = 's';
    rq_query1[6] = 'e';
    rq_query1[7] = 'a';
    rq_query1[8] = 'r';
    rq_query1[9] = 'c';
    rq_query1[10] = 'h';
    rq_query1[11] = ';';
    rq_query1[12] = 'a';
    rq_query1[13] = 'a';
    rq_query1[14] = 'a';
    rq_query1[15] = 'a';
    rq_query1[16] = 'a';
    rq_query1[17] = 'a';
    rq_query1[18] = 'a';
    rq_query1[19] = 'b';
    rq_query1[20] = ';';
    rq_query1[21] = 'a';
    rq_query1[22] = 'a';
    rq_query1[23] = 'a';
    rq_query1[24] = 'a';
    rq_query1[25] = 'a';
    rq_query1[26] = 'a';
    rq_query1[27] = 'a';
    rq_query1[28] = 'g';
    rq_query1[len_rg_query-1] = ';';
    rq_query1[len_rg_query] = '\0';
    //for (int i = 0; i < num_warmup; i++) {
    auto total2 = std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
    start = std::chrono::steady_clock::now();
    std::thread t2(receive_result, sock_cli);
    t2.detach();
    for (int i = 0; i < new_test_num; i++) {
        //
        int* rand_list;
        rand_list = range_gen();
        for(int o = 0; o < k_byte; o++) {
            rq_query[o+12] = static_cast<char>(97+rand_list[o]);
        }
        rq_query[12+k_byte] = ';';
        for(int o = 0; o < k_byte; o++) {
            rq_query[o+12+k_byte+1] = static_cast<char>(97+rand_list[o+k_byte]);
        }
        //
        //cout << "[" << i << "] rqw\n" << rq_query << endl;
        send(sock_cli, rq_query, strlen(rq_query), 0);
        //send(sock_cli, rq_query1, strlen(rq_query1), 0);
        //char recvbuf[BUFFER_SIZE];
        //int len;
        //len = recv(sock_cli, recvbuf, sizeof(recvbuf), 0);
        //memset(recvbuf, 0, sizeof(recvbuf));
    }
    send(sock_cli, check_info, strlen(check_info), 0);
    stop = std::chrono::steady_clock::now();
    total2 += std::chrono::duration_cast<std::chrono::nanoseconds>(stop - start).count();
    //cout << "Warmup completed." << endl;
    //test rs
    //double latency = total / (round * num_query);
    //cout << "Point Search: " << latency << " s" << endl;    
    //cout << "Range Search: " << total2 << " nano seconds" << endl;
    //cout << "# of Op.: " << new_test_num << endl;
    //cout << "Completed...\n\n" << endl;
    //cout << total2 << endl;
    

    sleep(3);
    char exit_info[5];
    exit_info[0] = 'e';
    exit_info[1] = 'x';
    exit_info[2] = 'i';
    exit_info[3] = 't';
    exit_info[4] = ';';
    exit_info[5] = '\0';
    send(sock_cli, exit_info, strlen(exit_info), 0);

    /*
    while(1){
        //把可读文件描述符的集合清空
        FD_ZERO(&rfds);
        //把标准输入的文件描述符加入到集合中
        FD_SET(0, &rfds);
        maxfd = 0;
        //把当前连接的文件描述符加入到集合中
        FD_SET(sock_cli, &rfds);
        //找出文件描述符集合中最大的文件描述符   
        if(maxfd < sock_cli)
            maxfd = sock_cli;
        //设置超时时间
        tv.tv_sec = 5;
        tv.tv_usec = 0;
        //等待聊天
        retval = select(maxfd+1, &rfds, NULL, NULL, &tv);
        if(retval == -1)
        {
            printf("select出错, 客户端程序退出\n");
            break;
        }
        else if(retval == 0)
        {
            printf("[5s]");
            continue;
        }
        else
        {
            //服务器发来了消息
            if(FD_ISSET(sock_cli,&rfds))
            {
                char recvbuf[BUFFER_SIZE];
                int len;
                len = recv(sock_cli, recvbuf, sizeof(recvbuf),0);
                printf("%s", recvbuf);
                memset(recvbuf, 0, sizeof(recvbuf));
            }
            //用户输入信息了,开始处理信息并发送
            if(FD_ISSET(0, &rfds))
            {
                char sendbuf[BUFFER_SIZE];
                fgets(sendbuf, sizeof(sendbuf), stdin);
                send(sock_cli, sendbuf, strlen(sendbuf),0); //发送
                memset(sendbuf, 0, sizeof(sendbuf));
            }
        }
    }
    */

    close(sock_cli);
    return 0;
}