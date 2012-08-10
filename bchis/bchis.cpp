// bchis.cpp : Defines the entry point for the console application.
//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <map>
#include "sqlite3.h"
#include "w3c.h"

static char SQLCreateTable[] = "CREATE TABLE IF NOT EXISTS "
"history(timestamp INTEGER, price REAL, amount REAL, PRIMARY KEY(timestamp ASC))";

static char SQLInsertTable[] = "INSERT OR REPLACE INTO history VALUES(%ld, %lf, %lf)";

typedef int BOOL;
#define TRUE 1
#define FALSE 0
typedef struct{
	long timestamp;
	double price;
	double amount;
}s_trade;

typedef struct{
	long timestamp;
	double open;
	long open_timestamp;
	double high;
	double low;
	double close;
	long close_timestamp;
	double amount;
}s_history;

void read_data_from_file(void);
void read_data_from_db(void);
sqlite3 *pDB;
char *errmsg;

typedef std::map<long, s_history> DataMap;
DataMap dataMap;

int main(int argc, char* argv[])
{
	int retcode;
	
	retcode = sqlite3_open("history.db", &pDB);
	if(SQLITE_OK!=retcode){
		printf("retcode of sqlite3_open():%d description:%s", retcode, sqlite3_errmsg(pDB));
		sqlite3_close(pDB);
		return 1;
	}
	
	retcode = sqlite3_exec(pDB, SQLCreateTable, 0, NULL, &errmsg);
	if(SQLITE_OK!=retcode){
		printf("retcode of sqlite3_exec():%d description:%s", retcode, errmsg);
		sqlite3_free(errmsg);
	}
	
	FILE *fp = fopen("trades.csv", "wb");
	if(NULL != fp){
		W3Client w3;
		if(w3.Connect("http://bitcoincharts.com")){
			if(w3.Request("/t/trades.csv?symbol=mtgoxUSD")){
				// 默认获得前五天的数据
				// 如果使用"trades.csv?symbol=mtgoxUSD&start=0",则获取全部数据
				char buf[1024];
				for(;;){
					int len = w3.Response(reinterpret_cast<unsigned char *>(buf), sizeof(buf));
					if(0 == len){
						break;
					}
					buf[len] = '\0';
					printf(buf);
					fwrite(buf, len, 1, fp);
				}
			}
			w3.Close();
		}
		fclose(fp);
	}
	printf("\nFinish http-get ...\n");
	
	read_data_from_file();
	read_data_from_db();
	
	sqlite3_close(pDB);
	
	printf("\nFinish write files.\n");
	return 0;
}

/** sqlite3_exec的回调。
*
*  向控制台打印查询的结果。
*
*  @param in data 传递给回调函数的数据。
*  @param in n_columns sqlite3_exec执行结果集中列的数量。
*  @param in col_values sqlite3_exec执行结果集中每一列的数据。
*  @param in col_names sqlite3_exec执行结果集中每一列的名称。
*  @return 状态码。
*/
int sqlite3_exec_callback(void *data, int n_columns, char **col_values, char **col_names)
{
	s_trade trade;
	long time_peroid = 0;
    /*
	for (int i = 0; i < n_columns; i++)
    {
	printf("%s:%s\t", col_names[i], col_values[i]);
    }
	*/
	trade.timestamp = atol(col_values[0]);
	trade.price = atof(col_values[1]);
	trade.amount = atof(col_values[2]);
	
    if(0 == strcmp((const char *)data, "5m")){
		time_peroid = 5*60;
	} else if(0 == strcmp((const char *)data, "15m")){
		time_peroid = 15*60;
	} else if(0 == strcmp((const char *)data, "30m")){
		time_peroid = 30*60;
	} else if(0 == strcmp((const char *)data, "1h")){
		time_peroid = 60*60;
	} else if(0 == strcmp((const char *)data, "4h")){
		time_peroid = 60*60;
	} else if(0 == strcmp((const char *)data, "1d")){
		time_peroid = 60*60*24;
	}
	
	if(0 != time_peroid){
		long time = ((long)(trade.timestamp/time_peroid))*time_peroid;
		DataMap::iterator it = dataMap.find(time);
		if(it==dataMap.end())
		{
			s_history history;
			history.amount = trade.amount;
			history.open = trade.price;
			history.close = trade.price;
			history.high = trade.price;
			history.low = trade.price;
			history.timestamp = time;
			history.open_timestamp = trade.timestamp;
			history.close_timestamp = trade.timestamp;
			dataMap.insert(std::make_pair(time, history));
		} else {
			it->second.amount += trade.amount;
			if(trade.price > it->second.high){
				it->second.high = trade.price;
			}
			if(trade.price < it->second.low){
				it->second.low = trade.price;
			}
			if(trade.timestamp < it->second.open_timestamp){
				it->second.open = trade.price;
				it->second.open_timestamp = trade.timestamp;
			}
			if(trade.timestamp > it->second.close_timestamp){
				it->second.close = trade.price;
				it->second.close_timestamp = trade.timestamp;
			}
		}
	}
	
    return 0;
}

void writeout_file(char *time_peroid){
	char sql[1024];
	int retcode;
	
	sprintf(sql, "SELECT * FROM history");
	retcode = sqlite3_exec(pDB, sql, &sqlite3_exec_callback, time_peroid, &errmsg);
	if(SQLITE_OK!=retcode){
		printf("retcode of sqlite3_exec():%d description:%s", retcode, errmsg);
		sqlite3_free(errmsg);
	}
	
	char filename[1024];
	sprintf(filename, "%s.csv", time_peroid);
	FILE *fp = fopen(filename, "wt");
	if(NULL!=fp){
		printf("\nWriting file:%s...", filename);
		for(DataMap::iterator it = dataMap.begin(); it != dataMap.end(); it++){
			struct tm *st = localtime(&(it->second.timestamp));
			fprintf(fp, "%04d.%02d.%02d,%02d:%02d,%lf,%lf,%lf,%lf,%lf\n", 
				st->tm_year+1900, st->tm_mon+1, st->tm_mday, st->tm_hour, st->tm_min,
				it->second.open, it->second.high, it->second.low, it->second.close,
				it->second.amount);
				/*
				printf("timestamp:%s open:%lf high:%lf low:%lf close:%lf amount:%lf\n",
				ctime(&(it->second.timestamp)), it->second.open, it->second.high,
				it->second.low, it->second.close, it->second.amount);
			*/
			
		}
		printf("done", filename);
		fclose(fp);
	}
	
	dataMap.clear(); // clear all the data in map
}

void read_data_from_db(void){
	writeout_file("5m");
	writeout_file("15m");
	writeout_file("30m");
	writeout_file("1h");
	writeout_file("4h");
	writeout_file("1d");
}

void read_data_from_file(void){
	s_trade trade;
	int retcode;
	FILE *fp=fopen("trades.csv", "rt");
	if(NULL != fp){
		char buffer[1024];
		
		sqlite3_exec(pDB, "BEGIN;", 0, NULL, &errmsg);
		for(;;){
			char *p = fgets(buffer, sizeof(buffer), fp);
			if(NULL == p){
				break;
			}
			
			p = strtok(buffer, ",");
			BOOL is_all_var_set = FALSE;
			for(int i=0;NULL!=p;i++){
				if(0==i){
					trade.timestamp = atol(p);
				} else if(1==i){
					trade.price = atof(p);
				} else if(2==i){
					trade.amount = atof(p);
					is_all_var_set = TRUE;
				}
				p = strtok(NULL, ",");
			}
			
			if(TRUE == is_all_var_set){
				// check timestamp is exist in DB
				// insert data in DB
				char sql_insert[1024];
				sprintf(sql_insert, SQLInsertTable, trade.timestamp, trade.price, trade.amount);
				retcode = sqlite3_exec(pDB, sql_insert, 0, NULL, &errmsg);
				if(SQLITE_OK!=retcode){
					printf("retcode of sqlite3_exec():%d description:%s", retcode, errmsg);
					sqlite3_free(errmsg);
				}
			} else {
				// 一行数据不完整，忽略
			}
		}
		sqlite3_exec(pDB, "END;", 0, NULL, &errmsg);
		fclose(fp);
	} else {
		printf("Cannot open source file.");
	}
}
