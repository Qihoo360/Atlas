#include <mysql/errmsg.h>
#include <mysql/mysql.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#define MAX_LEN 1024
#define SQL_LEN 1024
#define RES_LEN 1024

int type = 0;
int n_thread = 1;
char host[MAX_LEN] = "127.0.0.1";
int port = 3306;
char user[MAX_LEN] = "";
char passwd[MAX_LEN] = "";
char sqls[MAX_LEN] = "sql_list";
int n_query = 100;
char ch[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
int n_select, n_insert, n_update;
double t_select, t_insert, t_update; 

bool diff(char* line, MYSQL_ROW row, unsigned n)	//比较一行结果
{
	if (row == NULL) return false;

	line[strlen(line)-1] = '\0';	//去除尾部的回车符

	char* p = line;
	char* q = strchr(p, '\t');
	unsigned i = 0;

	while (q != NULL)
	{
		*q = '\0';
		if (strcmp(p, row[i]) != 0)
		{
		//	printf("p = %s\t%lu\n", p, strlen(p));
		//	printf("r = %s\t%lu\n", row[i], strlen(row[i]));
			return false;
		}
		p = q + 1;
		q = strchr(p, '\t');
		++i;
	}
	if (strcmp(p, row[i]) != 0)
	{
	//	printf("p = %s\t%lu\n", p, strlen(p));
	//	printf("r = %s\t%lu\n", row[i], strlen(row[i]));
		return false;
	}

	return true;
}

void skip(FILE* file)	//跳过剩余的结果
{
	char text[SQL_LEN] = {0};
	while (fgets(text, SQL_LEN, file) != NULL)	//读一行SQL
	{
		if (strcmp(text, "\n") == 0) break;
	}
}

void* test_verify(void* arg)
{
	//创建连接
	MYSQL mysql;
	mysql_init(&mysql);
	if (mysql_real_connect(&mysql, host, user, passwd, NULL, port, NULL, 0) == NULL)
	{
		printf("Failed to connect mysql\n");
		return NULL;
	}

	//打开SQL文件
	FILE* file = fopen(sqls, "r");
	if (file == NULL)
	{
		printf("Failed to open sql file\n");
		return NULL;
	}

	char query[SQL_LEN];
	while (fgets(query, SQL_LEN, file) != NULL)	//读一行SQL
	{
		if (strcmp(query, "\n") == 0) continue;	//读到空行，跳过

		query[strlen(query)-1] = '\0';	//去除尾部的回车符

		if (strcasestr(query, "use") == query)	//USE DB的情况
		{
			struct timeval start, end;
			gettimeofday(&start, NULL);
			int ret = mysql_select_db(&mysql, query+4);
			gettimeofday(&end, NULL);
			double interval = (end.tv_sec - start.tv_sec)*1000 + (end.tv_usec - start.tv_usec)/1000.0;//单位ms
			if (ret == 0) printf("Execute Succeed(%.3f ms): %s\n\n", interval, query);
			else printf("Execute Failed(%.3f ms): %s\n\n", interval, query);
			continue;
		}

		//执行
		struct timeval start, end;
		gettimeofday(&start, NULL);
		if (mysql_query(&mysql, query) != 0)
		{
			char msg[RES_LEN];
			if (fgets(msg, SQL_LEN, file) != NULL && strcasecmp(msg, "error\n") == 0) printf("Execute Blocked: %s\n\n", query);
			else printf("Execute Failed: %s\n\n", query);
			continue;
		}
		gettimeofday(&end, NULL);
		double interval = (end.tv_sec - start.tv_sec)*1000 + (end.tv_usec - start.tv_usec)/1000.0;	//单位ms

		//对比结果
		MYSQL_RES* result = mysql_store_result(&mysql);
		if (result != NULL)	//SELECT语句
		{
		//	printf("num = %llu\n", mysql_num_rows(result));
			bool not_equal = false;
			char line[RES_LEN];

			while (fgets(line, RES_LEN, file) != NULL && strcmp(line, "\n") != 0)
			{
				MYSQL_ROW row = mysql_fetch_row(result);

				if (diff(line, row, mysql_num_fields(result)) == false)	//某一行结果不一致
				{
					not_equal = true;
					break;
				}
			}

			if (not_equal == true)
			{
				printf("Result Incorrect(%.3f ms): %s\n\n", interval, query);
				skip(file);
			}
			else
			{
				if (mysql_fetch_row(result) != NULL)	//实际结果行数多于预料
				{
					printf("Result Incorrect(%.3f ms): %s\n\n", interval, query);
				}
				else	//实际结果与预料行数相等且每行都相同
				{
					printf("Result  Correct(%.3f ms): %s\n\n", interval, query);
				}
			}

			mysql_free_result(result);
		}
		else	//INSERT或UPDATE语句
		{
			printf("Result  Correct(%.3f ms): %s\n\n", interval, query);
		}
	}

	//清理
	fclose(file);
	mysql_close(&mysql);
	return NULL;
}

void print_error(MYSQL mysql, char* query)
{
	switch (mysql_errno(&mysql))
	{
	case CR_COMMANDS_OUT_OF_SYNC:
		printf("CR_COMMANDS_OUT_OF_SYNC: ");
		break;
	case CR_SERVER_GONE_ERROR:
		printf("CR_SERVER_GONE_ERROR: ");
		break;
	case CR_SERVER_LOST:
		printf("CR_SERVER_LOST: ");
		break;
	case CR_UNKNOWN_ERROR:
		printf("CR_UNKNOWN_ERROR: ");
		break;
	}

	printf("%s\n\n", query);
	exit(3);
}

void* test_short(void* arg)
{
	//创建连接
	MYSQL mysql;
	mysql_init(&mysql);

	char query[SQL_LEN];
	char name[17];
	name[16] = '\0';
	for (int i = 0; i < n_query; ++i)
	{
		if (mysql_real_connect(&mysql, host, user, passwd, NULL, port, NULL, 0) == NULL)
		{
			printf("Failed to connect mysql\n");
			return NULL;
		}

	//随机生成一条SQL
		int mode = rand() % 6;
		switch (mode)
		{
		case 0:		//SELECT
		case 1:
		case 2:
		case 3:
			sprintf(query, "SELECT * FROM person.mytable WHERE id = %d", rand());
			++n_select;
			break;
		case 4:		//INSERT
			for (int j = 0; j < 16; ++j)
			{
				name[j] = ch[rand() % 16];
			}
			sprintf(query, "INSERT INTO person.mytable VALUES (%d, '%s')", rand(), name);
			++n_insert;
			break;
		case 5:		//UPDATE
			for (int j = 0; j < 16; ++j)
			{
				name[j] = ch[rand() % 16];
			}
			sprintf(query, "UPDATE person.mytable SET name = '%s' WHERE id = %d", name, rand());
			++n_update;
			break;
		}
	//mysql_query
	//	printf("%s\n", query);

		struct timeval start, end;
		gettimeofday(&start, NULL);

		if (mysql_query(&mysql, query) != 0)
		{
			print_error(mysql, query);
		}

		gettimeofday(&end, NULL);
		double interval = (end.tv_sec - start.tv_sec)*1000 + (end.tv_usec - start.tv_usec)/1000.0;//单位ms
		if (mode <= 3) t_select += interval;
		else if (mode == 4) t_insert += interval;
		else t_update += interval;

		mysql_close(&mysql);
	}

	return NULL;
}

void* test_long(void* arg)
{
	//创建连接
	MYSQL mysql;
	mysql_init(&mysql);
	if (mysql_real_connect(&mysql, host, user, passwd, NULL, port, NULL, 0) == NULL)
	{
		printf("Failed to connect mysql\n");
		return NULL;
	}

	if (mysql_select_db(&mysql, "person") != 0)
	{
		printf("Failed to use db\n");
		return NULL;
	}

	char query[SQL_LEN];
	char name[17];
	name[16] = '\0';
	for (int i = 0; i < n_query; ++i)
	{
	//随机生成一条SQL
		int mode = rand() % 6;
		switch (mode)
		{
		case 0:		//SELECT
		case 1:
		case 2:
		case 3:
			sprintf(query, "SELECT * FROM mytable WHERE id = %d", rand());
			++n_select;
			break;
		case 4:		//INSERT
			for (int j = 0; j < 16; ++j)
			{
				name[j] = ch[rand() % 16];
			}
			sprintf(query, "INSERT INTO mytable VALUES (%d, '%s')", rand(), name);
			++n_insert;
			break;
		case 5:		//UPDATE
			for (int j = 0; j < 16; ++j)
			{
				name[j] = ch[rand() % 16];
			}
			sprintf(query, "UPDATE mytable SET name = '%s' WHERE id = %d", name, rand());
			++n_update;
			break;
		}
	//mysql_query
	//	printf("%s\n", query);

		struct timeval start, end;
		gettimeofday(&start, NULL);

		if (mysql_query(&mysql, query) != 0)
		{
			print_error(mysql, query);
		}

		gettimeofday(&end, NULL);
		double interval = (end.tv_sec - start.tv_sec)*1000 + (end.tv_usec - start.tv_usec)/1000.0;//单位ms

		if (mode <= 3)
		{
			mysql_free_result(mysql_use_result(&mysql));
			t_select += interval;
		}
		else if (mode == 4)
		{
			t_insert += interval;
		}
		else
		{
			t_update += interval;
		}
	}

	mysql_close(&mysql);
	return NULL;
}

bool get_options(int argc, char** argv)
{
	int n = 0;
	for (int i = 1; i < argc-1; ++i)
	{
		if (argv[i][0] == '-')
		{
			char ch = argv[i][1];
			if (ch == '\0') return false;

			char* p = argv[i]+2;
			if (*p == '\0')
			{
				p = argv[++i];
			}

			switch(ch)
			{
			case 't':
				if (strcasecmp(p, "verify") == 0) type = 1;
				else if (strcasecmp(p, "short") == 0) type = 2;
				else if (strcasecmp(p, "long") == 0) type = 3;
				else return false;
				n |= 1;
				break;
			case 'c':
				n_thread = atoi(p);
				n |= 2;
				break;
			case 'h':
				strcpy(host, p);
				n |= 4;
				break;
			case 'P':
				port = atoi(p);
				n |= 8;
				break;
			case 'u':
				strcpy(user, p);
				n |= 16;
				break;
			case 'p':
				strcpy(passwd, p);
				n |= 32;
				break;
			case 'f':
				strcpy(sqls, p);
				break;
			case 'n':
				n_query = atoi(p);
				break;
			default:
				return false;
			}
		}
	}
/*
	printf("type = %s\n", type);
	printf("n_thread = %d\n", n_thread);
	printf("host = %s\n", host);
	printf("port = %d\n", port);
	printf("user = %s\n", user);
	printf("pwd = %s\n", passwd);
	printf("sqls = %s\n", sqls);
*/
	if (n != 63) return false;
	return true;
}

int main(int argc, char** argv)
{
	if (get_options(argc, argv) == false)
	{
		printf("usage: %s -t verify|performance -c threads -h host -P port -u username -p password -f sql_file\n", argv[0]); 
		printf("example: %s -t verify -c 10 -h 127.0.0.1 -P 4040 -u qtbuser -p qihoo.net -f sql_list\n", argv[0]); 
		return 1; 
	}

	struct timeval tp;
	gettimeofday(&tp, NULL);
	srand(tp.tv_usec);

	char kind[8];
	void* (*func)(void*);
	switch (type)
	{
	case 1:
		strcpy(kind, "VERIFY");
		func = test_verify;
		break;
	case 2:
		strcpy(kind, "SHORT CONNECTION");
		func = test_short;
		break;
	case 3:
		strcpy(kind, "LONG CONNECTION");
		func = test_long;
		break;
	}

	n_select = n_insert = n_update = 0;
	t_select = t_insert = t_update = 0;

	printf("\n*************** TEST OF %s START ***************\n\n", kind);

	if (type == 1) n_thread = 1;

	pthread_t tid[n_thread];

	for (int i = 0; i < n_thread; ++i)
	{
		if (pthread_create(tid+i, NULL, func, NULL) != 0)
		{
			printf("failed to create thread\n");
			return 2;
		}
	}

	for (int i = 0; i < n_thread; ++i)
	{
		pthread_join(tid[i], NULL);
	}

	if (type != 1)
	{
		printf("Overall time: %.3f ms\n", t_select + t_insert + t_update);
		printf("Num of queries: %d\n", n_thread * n_query);
		printf("Average latency: %.3f ms\n\n", (t_select + t_insert + t_update) / n_thread / n_query);

		printf("Time of SELECTs: %.3f ms\n", t_select);
		printf("Num of SELECTs: %d\n", n_select);
		printf("Average latency of SELECTs: %.3f ms\n\n", t_select / n_select);

		printf("Time of INSERTs: %.3f ms\n", t_insert);
		printf("Num of INSERTs: %d\n", n_insert);
		printf("Average latency of INSERTs: %.3f ms\n\n", t_insert / n_insert);

		printf("Time of UPDATEs: %.3f ms\n", t_update);
		printf("Num of UPDATEs: %d\n", n_update);
		printf("Average latency of UPDATEs: %.3f ms\n\n", t_update / n_update);
	}

	printf("*************** TEST OF %s END ***************\n\n", kind);

	return 0;
}
