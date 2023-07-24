// SPDX-License-Identifier: GPL-2.0-only
/*
 * Copyright (C) 2023  Alviro Iskandar Setiawan <alviro.iskandar@gnuweeb.org>
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <mysql/mysql.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <math.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <pthread.h>
#include <stdint.h>
#include <signal.h>
#include <stdatomic.h>

#define BUFFER_SIZE		(32ull*1024ull*1024ull)
#define NR_ROWS_BUF		(8192)
#define NR_MYSQL_WORKERS	(16)

struct dt_lmarks {
	uint32_t	*marks;
	size_t		nr_marks;
};

enum {
	T_UNKNOWN   = 0,
	T_INTEGER   = 1,
	T_FLOAT     = 2,
	T_DATE      = 3,
	T_DATETIME  = 4,
	T_VARCHAR   = 5,
};

enum {
	MODE_DML,
	MODE_DDL,
};

struct dt_col_heuristic {
	uint8_t		type;
	bool		is_nullable;
	bool		has_var_len;
	size_t		max_len;
};

struct dt_columns {
	char			**cols;
	struct dt_col_heuristic	*hr;
	size_t			nr_cols;
};

struct dt_row {
	char	**data;
	size_t	nr_cols;
};

struct dt_rows {
	struct dt_row	*rows;
	size_t		nr_rows;

	/*
	 * Avoid reallocating rows too often.
	 */
	size_t		nr_alloc;
};

struct mysql_wrk;

struct mysql_wrk {
	pthread_t		thread;
	MYSQL			*conn;
	void 			*arg;
	void			(*cb)(struct mysql_wrk *wrk, void *arg);
	volatile bool		is_used;
	struct mysql_wrk_pool	*pool;
	uint32_t		tid;
};

struct mysql_wrk_pool {
	struct mysql_wrk	*wrk;
	size_t			nr_wrk;
	pthread_mutex_t		lock;
	pthread_cond_t		cond;
	_Atomic(uint32_t)	nr_ready_wrk;
};

struct work_data {
	struct dt_columns	*col;
	struct dt_row		*rows;
	struct worker		*worker;
	size_t			nr_rows;
};

struct worker {
	FILE			*fp;

	/*
	 * Line markers. Used to determine the row width.
	 */
	struct dt_lmarks	lmarks;

	/*
	 * Column names.
	 */
	struct dt_columns	columns;

	/*
	 * Buffered rows. Not the entire table.
	 */
	struct dt_rows		rows;

	uint64_t		nr_lines;

	struct mysql_wrk_pool	mysql_pool;

	uint32_t		nr_pending_works;
	pthread_mutex_t		pending_wrk_lock;
	pthread_cond_t		pending_wrk_cond;
	const char		*table_name;
};

struct mysql_cred {
	char	*host;
	char	*user;
	char	*pass;
	char	*db;
	uint16_t port;
};

static struct mysql_cred mysql_cred;
static uint8_t run_mode;
static volatile bool g_stop;

static inline void *ERR_PTR(long error)
{
	return (void *) error;
}

static inline long PTR_ERR(const void *ptr)
{
	return (long) ptr;
}

static void free_dt_lmarks(struct dt_lmarks *lmarks)
{
	free(lmarks->marks);
	lmarks->marks = NULL;
	lmarks->nr_marks = 0;
}

static void free_dt_columns(struct dt_columns *col)
{
	size_t i;

	for (i = 0; i < col->nr_cols; i++)
		free(col->cols[i]);

	free(col->hr);
	free(col->cols);
	col->cols = NULL;
	col->nr_cols = 0;
}

static void free_dt_rows(struct dt_rows *rows)
{
	size_t i, j;

	for (i = 0; i < rows->nr_rows; i++) {
		for (j = 0; j < rows->rows[i].nr_cols; j++)
			free(rows->rows[i].data[j]);

		free(rows->rows[i].data);
	}

	free(rows->rows);
	rows->rows = NULL;
	rows->nr_rows = 0;
	rows->nr_alloc = 0;
}

static void *trim_ws(char *str)
{
	unsigned char *head, *tail;
	size_t len;

	head = (unsigned char *)str;
	while (*head == ' ' || *head == '\t' || *head > 0x80)
		head++;

	/*
	 * All spaces?
	 */
	len = strlen((char *)head);
	if (!len) {
		*str = '\0';
		return str;
	}

	tail = head + len - 1;
	while (*tail == ' ' || *tail == '\t' || *tail > 0x80)
		tail--;

	tail[1] = '\0';
	if ((char *)head != str)
		return memmove(str, head, tail - head + 2);

	return str;
}

/*
 * @line may be modified. We don't duplicate it for performance reasons.
 */
static int parse_lmarks(char *line, struct dt_lmarks *lmarks)
{
	char *head, *tail;
	int err;

	head = tail = line;
	while (1) {
		uint32_t *tmp, len;

		while (*tail == '-')
			tail++;

		len = (uint32_t)(tail - head);
		if (!len)
			break;

		if (*tail != '\0' && *tail != ' ') {
			fprintf(stderr, "Invalid tail in parse_lmarks: %c\n", *tail);
			err = -EINVAL;
			goto out_err;
		}

		lmarks->nr_marks++;
		tmp = realloc(lmarks->marks, lmarks->nr_marks * sizeof(*tmp));
		if (!tmp) {
			fprintf(stderr, "Failed to allocate marks in parse_lmarks\n");
			err = -ENOMEM;
			goto out_err;
		}

		lmarks->marks = tmp;
		lmarks->marks[lmarks->nr_marks - 1] = len;

		if (*tail == '\0')
			break;

		head = ++tail;
	}

	return 0;

out_err:
	free_dt_lmarks(lmarks);
	return err;
}

static int parse_columns(char *line, struct dt_columns *col, struct dt_lmarks *lmarks)
{
	int ret = 0;
	size_t i;

	for (i = 0; i < lmarks->nr_marks; i++) {
		char *tmp, **tmp_cols;

		tmp = strndup(line, lmarks->marks[i]);
		if (!tmp) {
			fprintf(stderr, "Failed to duplicate column name\n");
			ret = -ENOMEM;
			goto out_err;
		}

		col->nr_cols++;
		tmp_cols = realloc(col->cols, col->nr_cols * sizeof(*tmp_cols));
		if (!tmp_cols) {
			free(tmp);
			fprintf(stderr, "Failed to allocate columns\n");
			ret = -ENOMEM;
			goto out_err;
		}

		tmp = trim_ws(tmp);
		col->cols = tmp_cols;
		col->cols[col->nr_cols - 1] = tmp;
		line += lmarks->marks[i] + 1;
	}

	return ret;

out_err:
	free_dt_columns(col);
	return ret;
}

static int scale_up_dt_rows(struct dt_rows *rows)
{
	struct dt_row *tmp;

	rows->nr_alloc += NR_ROWS_BUF;
	tmp = realloc(rows->rows, rows->nr_alloc * sizeof(*tmp));
	if (!tmp) {
		fprintf(stderr, "Failed to allocate rows\n");
		return -ENOMEM;
	}

	rows->rows = tmp;
	return 0;
}

static int parse_row(char *line, struct dt_rows *rows, struct dt_lmarks *lmarks)
{
	struct dt_row *row;
	int ret = 0;
	size_t i;

	if (rows->nr_rows >= rows->nr_alloc) {
		ret = scale_up_dt_rows(rows);
		if (ret)
			goto out_err;
	}

	if (line[0] == '\0')
		return -EAGAIN;

	row = &rows->rows[rows->nr_rows++];

	row->nr_cols = 0;
	row->data = calloc(lmarks->nr_marks, sizeof(*row->data));
	if (!row->data) {
		fprintf(stderr, "Failed to allocate row data\n");
		ret = -ENOMEM;
		goto out_err;
	}

	for (i = 0; i < lmarks->nr_marks; i++) {
		char *tmp;

		tmp = strndup(line, lmarks->marks[i]);
		if (!tmp) {
			fprintf(stderr, "Failed to duplicate row data\n");
			ret = -ENOMEM;
			goto out_err;
		}

		tmp = trim_ws(tmp);
		line += lmarks->marks[i] + 1;
		row->data[row->nr_cols++] = tmp;
	}

	return ret;

out_err:
	free_dt_rows(rows);
	return ret;
}

static char *fgets_kill_crlf(char *buf, size_t size, FILE *fp)
{
	size_t len;
	char *ret;

	ret = fgets(buf, size, fp);
	if (!ret)
		return NULL;

	len = strlen(buf);
	if (len && buf[len - 1] == '\n') {
		buf[len - 1] = '\0';
		len--;
	}

	if (len && buf[len - 1] == '\r') {
		buf[len - 1] = '\0';
		len--;
	}

	return ret;
}

static void column_init_heuristic(struct dt_columns *col)
{
	struct dt_col_heuristic *hr;
	size_t i;

	for (i = 0; i < col->nr_cols; i++) {
		hr = &col->hr[i];
		hr->type = T_UNKNOWN;
		hr->is_nullable = false;
		hr->max_len = 0;
	}
}

static bool is_floating_point(const char *str)
{
	const char *end = str + strlen(str);
	char *tmp;

	strtod(str, &tmp);
	if (tmp != end)
		return false;

	return true;
}

static bool is_integer(const char *str)
{
	while (*str) {
		if (*str < '0' || *str > '9')
			return false;

		str++;
	}

	return true;
}

static bool is_date(const char *str)
{
	int year, month, day;

	if (sscanf(str, "%d-%d-%d", &year, &month, &day) != 3)
		return false;

	if (year < 1900 || year > 2100)
		return false;

	if (month < 1 || month > 12)
		return false;

	if (day < 1 || day > 31)
		return false;

	return true;
}

static bool is_datetime(const char *str)
{
	int year, month, day, hour, minute, second;

	if (sscanf(str, "%d-%d-%d %d:%d:%d", &year, &month, &day, &hour, &minute, &second) != 6)
		return false;

	if (year < 1900 || year > 2100)
		return false;

	if (month < 1 || month > 12)
		return false;

	if (day < 1 || day > 31)
		return false;

	if (hour < 0 || hour > 23)
		return false;

	if (minute < 0 || minute > 59)
		return false;

	if (second < 0 || second > 59)
		return false;

	return true;
}

static void column_detect_heuristic_row_cmp(struct dt_col_heuristic *hr, const char *str)
{
	size_t len;

	if (!strcmp(str, "NULL")) {
		hr->is_nullable = true;
		return;
	}

	len = strlen(str);
	if (len > hr->max_len)
		hr->max_len = len;

	if (hr->type != T_UNKNOWN && len != hr->max_len)
		hr->has_var_len = true;

	if (hr->type == T_UNKNOWN) {
		if (is_integer(str))
			hr->type = T_INTEGER;
		else if (is_floating_point(str))
			hr->type = T_FLOAT;
		else if (is_date(str))
			hr->type = T_DATE;
		else if (is_datetime(str))
			hr->type = T_DATETIME;
		else
			hr->type = T_VARCHAR;

		return;
	}

	if (hr->type == T_VARCHAR)
		return;

	if (hr->type == T_INTEGER) {
		if (is_integer(str))
			return;

		if (is_floating_point(str))
			hr->type = T_FLOAT;
		else
			hr->type = T_VARCHAR;

		return;
	}

	if (hr->type == T_FLOAT) {
		if (!is_floating_point(str))
			hr->type = T_VARCHAR;

		return;
	}

	if (hr->type == T_DATE) {
		if (!is_date(str))
			hr->type = T_VARCHAR;

		return;
	}

	if (hr->type == T_DATETIME) {
		if (!is_datetime(str))
			hr->type = T_VARCHAR;

		return;
	}

	// Must never happen.
	abort();
}

static int column_detect_heuristic_row(struct dt_columns *col, struct dt_row *row)
{
	struct dt_col_heuristic *hr;
	size_t i;

	if (col->nr_cols != row->nr_cols) {
		fprintf(stderr, "Column count mismatch\n");
		return -EINVAL;
	}

	for (i = 0; i < col->nr_cols; i++) {
		hr = &col->hr[i];
		column_detect_heuristic_row_cmp(hr, row->data[i]);
	}

	return 0;
}

static int column_detect_heuristic_rows(struct dt_columns *col, struct dt_rows *rows)
{
	size_t i;
	int ret;

	for (i = 0; i < rows->nr_rows; i++) {
		ret = column_detect_heuristic_row(col, &rows->rows[i]);
		if (ret)
			return ret;
	}

	return 0;
}

static int column_exec_heuristic(struct dt_columns *col, struct dt_rows *rows)
{
	if (!col->hr) {
		col->hr = calloc(col->nr_cols, sizeof(*col->hr));
		if (!col->hr) {
			fprintf(stderr, "Failed to allocate column heuristic\n");
			return -ENOMEM;
		}

		column_init_heuristic(col);
	}

	return column_detect_heuristic_rows(col, rows);
}

static struct mysql_wrk *get_mysql_worker(struct mysql_wrk_pool *pool)
{
	struct mysql_wrk *wrk;
	uint32_t i;

	for (i = 0; i < pool->nr_wrk; i++) {
		wrk = &pool->wrk[i];
		if (!wrk->is_used) {
			wrk->is_used = true;
			return wrk;
		}
	}

	return NULL;
}

static int calculate_buf_size(struct work_data *wd)
{
	int ret = (int)sizeof("INSERT INTO `TABLE_NAME` () VALUES ();");
	size_t i;

	ret += strlen(wd->worker->table_name);
	for (i = 0; i < wd->col->nr_cols; i++)
		ret += strlen(wd->col->cols[i]) + sizeof("``,  ");

	ret += wd->nr_rows * wd->col->nr_cols * sizeof("?  ") + sizeof(",  ");
	return ret;
}

static int build_query(struct work_data *wd, char **qp)
{
	char *buf, *orig;
	int ret, rem;
	size_t i, j;

	rem = calculate_buf_size(wd);
	orig = buf = malloc(rem);
	if (!buf) {
		fprintf(stderr, "Failed to allocate query\n");
		return -ENOMEM;
	}

	ret = snprintf(buf, rem, "INSERT INTO `%s` (", wd->worker->table_name);
	rem -= ret;
	buf += ret;

	for (i = 0; i < wd->col->nr_cols; i++) {
		ret = snprintf(buf, rem, "`%s`", wd->col->cols[i]);
		rem -= ret;
		buf += ret;

		if (i != wd->col->nr_cols - 1) {
			ret = snprintf(buf, rem, ", ");
			rem -= ret;
			buf += ret;
		}
	}

	ret = snprintf(buf, rem, ") VALUES ");
	rem -= ret;
	buf += ret;

	for (i = 0; i < wd->nr_rows; i++) {
		ret = snprintf(buf, rem, "(");
		rem -= ret;
		buf += ret;

		for (j = 0; j < wd->col->nr_cols; j++) {
			ret = snprintf(buf, rem, "?");
			rem -= ret;
			buf += ret;

			if (j != wd->col->nr_cols - 1) {
				ret = snprintf(buf, rem, ", ");
				rem -= ret;
				buf += ret;
			}
		}

		ret = snprintf(buf, rem, ")");
		rem -= ret;
		buf += ret;

		if (i != wd->nr_rows - 1) {
			ret = snprintf(buf, rem, ", ");
			rem -= ret;
			buf += ret;
		}
	}

	*qp = orig;
	return 0;
}

static int __insert_to_db(struct mysql_wrk *wrk, struct work_data *wd, const char *q)
{
	MYSQL_BIND *bind = NULL;
	bool *is_null = NULL;
	MYSQL_STMT *stmt;
	int ret = 0;
	size_t i, j;

	stmt = mysql_stmt_init(wrk->conn);
	if (!stmt) {
		fprintf(stderr, "Failed to initialize mysql statement\n");
		return -ENOMEM;
	}

	ret = mysql_stmt_prepare(stmt, q, strlen(q));
	if (ret) {
		fprintf(stderr, "Failed to prepare mysql statement: %s\n", mysql_error(wrk->conn));
		ret = -ENOMEM;
		goto out;
	}

	bind = calloc(wd->col->nr_cols * wd->nr_rows, sizeof(*bind));
	if (!bind) {
		fprintf(stderr, "Failed to allocate bind\n");
		ret = -ENOMEM;
		goto out;
	}

	is_null = calloc(wd->col->nr_cols * wd->nr_rows, sizeof(*is_null));
	if (!is_null) {
		fprintf(stderr, "Failed to allocate is_null\n");
		ret = -ENOMEM;
		goto out;
	}

	for (i = 0; i < wd->nr_rows; i++) {
		for (j = 0; j < wd->col->nr_cols; j++) {
			size_t idx = i * wd->col->nr_cols + j;
			MYSQL_BIND *b = &bind[idx];
			char *data = wd->rows[i].data[j];

			if (!strcmp(data, "NULL")) {
				is_null[idx] = true;
				b->buffer_type = MYSQL_TYPE_NULL;
				continue;
			}

			b->buffer_type = MYSQL_TYPE_STRING;
			b->buffer = data;
			b->buffer_length = strlen(data);
		}
	}

	ret = mysql_stmt_bind_param(stmt, bind);
	if (ret) {
		fprintf(stderr, "Failed to bind mysql statement: %s\n", mysql_error(wrk->conn));
		ret = -ENOMEM;
		goto out;
	}

	ret = mysql_stmt_execute(stmt);
	if (ret) {
		fprintf(stderr, "Failed to execute mysql statement: %s\n", mysql_error(wrk->conn));
		ret = -ENOMEM;
	}
out:
	free(is_null);
	free(bind);
	mysql_stmt_close(stmt);
	return ret;
}

static void post_db_work_callback(struct mysql_wrk *wrk, void *arg)
{
	struct work_data *wd = arg;
	struct worker *main_wrk = wd->worker;
	char *q = NULL;

	if (build_query(wd, &q)) {
		g_stop = true;
		goto out;
	}

	if (__insert_to_db(wrk, wd, q)) {
		g_stop = true;
		goto out;
	}

out:
	free(q);
	pthread_mutex_lock(&main_wrk->pending_wrk_lock);
	if (--main_wrk->nr_pending_works == 0)
		pthread_cond_signal(&main_wrk->pending_wrk_cond);
	pthread_mutex_unlock(&main_wrk->pending_wrk_lock);
	free(wd);
}

static int post_db_work(struct worker *wrk, struct work_data *wd)
{
	struct mysql_wrk_pool *pool = &wrk->mysql_pool;
	struct mysql_wrk *wrk_tmp;

	pthread_mutex_lock(&pool->lock);
	wrk_tmp = get_mysql_worker(pool);
	if (!wrk_tmp) {
		pthread_mutex_unlock(&pool->lock);
		return -EAGAIN;
	}

	wrk_tmp->is_used = true;
	wrk_tmp->arg = wd;
	wrk_tmp->cb = &post_db_work_callback;
	pthread_cond_broadcast(&pool->cond);
	pthread_mutex_unlock(&pool->lock);
	return 0;
}

static int insert_to_db(struct worker *wrk, struct dt_columns *col, struct dt_rows *rows)
{
	size_t nr_workers = wrk->mysql_pool.nr_wrk;
	size_t nr_rows = rows->nr_rows;
	struct timespec ts;
	size_t i;
	int ret = 0;

	pthread_mutex_lock(&wrk->pending_wrk_lock);
	for (i = 0; i < nr_workers; i++) {
		uint32_t nr_rows_to_post, idx;
		struct work_data *wd;

		idx = i * (nr_rows / nr_workers);
		if (i == nr_workers - 1)
			nr_rows_to_post = nr_rows - idx;
		else
			nr_rows_to_post = nr_rows / nr_workers;

		if (!nr_rows_to_post)
			break;

		wd = malloc(sizeof(*wd));
		if (!wd) {
			fprintf(stderr, "Failed to allocate work data\n");
			ret = -ENOMEM;
			break;
		}

		wd->worker = wrk;
		wd->col = col;
		wd->rows = rows->rows + idx;
		wd->nr_rows = nr_rows_to_post;

		printf("Posting work %zu: %zu rows\n", i, wd->nr_rows);
		post_db_work(wrk, wd);
	}
	wrk->nr_pending_works += (uint32_t) i;

	while (wrk->nr_pending_works) {
		printf("Waiting for %u pending works\n", wrk->nr_pending_works);

		clock_gettime(CLOCK_REALTIME, &ts);
		ts.tv_sec += 1;
		pthread_cond_timedwait(&wrk->pending_wrk_cond, &wrk->pending_wrk_lock, &ts);

		if (g_stop) {
			ret = -EINTR;
			break;
		}
	}
	printf("Continue parsing...\n");
	pthread_mutex_unlock(&wrk->pending_wrk_lock);

	return ret;
}

static int flush_rows(struct worker *wrk, struct dt_columns *col, struct dt_rows *rows)
{
	int ret = 0;

	if (run_mode == MODE_DDL)
		ret = column_exec_heuristic(col, rows);

	if (run_mode == MODE_DML)
		ret = insert_to_db(wrk, col, rows);

	free_dt_rows(rows);
	return ret;
}

static void dump_column_heuristic(struct dt_columns *col)
{
	struct dt_col_heuristic *hr;
	size_t i;

	printf("==============================\n");
	printf("=== Column Type Heuristic Result ===\n");
	printf("Total columns: %zu\n", col->nr_cols);
	printf("==============================\n");
	for (i = 0; i < col->nr_cols; i++) {
		hr = &col->hr[i];
		printf("%s: ", col->cols[i]);

		switch (hr->type) {
		case T_UNKNOWN:
			printf("UNKNOWN");
			break;
		case T_INTEGER:
			printf("INTEGER");
			break;
		case T_FLOAT:
			printf("FLOAT");
			break;
		case T_DATE:
			printf("DATE");
			break;
		case T_DATETIME:
			printf("DATETIME");
			break;
		case T_VARCHAR:
			if (hr->has_var_len)
				printf("VARCHAR(%zu)", hr->max_len);
			else
				printf("CHAR(%zu)", hr->max_len);
			break;
		default:
			abort();
		}

		if (hr->is_nullable)
			printf(" NULL");

		printf("\n");
	}
}

static void *worker_func(void *arg)
{
	struct worker *wrk = arg;
	void *ret = NULL;
	char *col = NULL;
	char *line, *buf;
	int err;

	buf = malloc(BUFFER_SIZE);
	if (!buf) {
		fprintf(stderr, "Failed to allocate buffer\n");
		return ERR_PTR(-ENOMEM);
	}

	/*
	 * The first line contains the column names.
	 */
	col = fgets_kill_crlf(buf, BUFFER_SIZE, wrk->fp);
	if (!col) {
		ret = ERR_PTR(-EINVAL);
		fprintf(stderr, "Failed to read the first line\n");
		goto out;
	}

	wrk->nr_lines++;
	col = strdup(col);
	if (!col) {
		ret = ERR_PTR(-ENOMEM);
		fprintf(stderr, "Failed to duplicate col line\n");
		goto out;
	}

	/*
	 * The second line contains line markers.
	 */
	line = fgets_kill_crlf(buf, BUFFER_SIZE, wrk->fp);
	if (!line) {
		ret = ERR_PTR(-EINVAL);
		fprintf(stderr, "Failed to read the second line\n");
		goto out;
	}

	wrk->nr_lines++;
	err = parse_lmarks(line, &wrk->lmarks);
	if (err) {
		ret = ERR_PTR(err);
		fprintf(stderr, "Failed to parse line markers\n");
		goto out;
	}

	err = parse_columns(col, &wrk->columns, &wrk->lmarks);
	if (err) {
		ret = ERR_PTR(err);
		fprintf(stderr, "Failed to parse columns\n");
		goto out;
	}

	free(col);
	col = NULL;

	while (!g_stop) {
		line = fgets_kill_crlf(buf, BUFFER_SIZE, wrk->fp);
		if (!line)
			break;

		wrk->nr_lines++;
		err = parse_row(line, &wrk->rows, &wrk->lmarks);
		if (err == -EAGAIN)
			break;

		if (err) {
			ret = ERR_PTR(err);
			fprintf(stderr, "Failed to parse row\n");
			goto out;
		}

		/*
		 * Don't let the buffer grow too big. (524288)
		 */
		if (wrk->nr_lines % 20480 == 0) {
			printf("Parsed %llu lines\n", (unsigned long long) wrk->nr_lines);
			err = flush_rows(wrk, &wrk->columns, &wrk->rows);
			if (err) {
				ret = ERR_PTR(err);
				goto out;
			}
		}
	}

	err = flush_rows(wrk, &wrk->columns, &wrk->rows);
	if (err) {
		ret = ERR_PTR(err);
		goto out;
	}

	printf("Total parsed lines: %llu\n", (unsigned long long) wrk->nr_lines);
	if (run_mode == MODE_DDL)
		dump_column_heuristic(&wrk->columns);
out:
	free_dt_columns(&wrk->columns);
	free_dt_lmarks(&wrk->lmarks);
	free_dt_rows(&wrk->rows);
	free(col);
	free(buf);
	return ret;
}

static int parse_mysql_env(struct mysql_cred *cred, struct worker *wrk)
{
	char *tmp;
	int port;

	tmp = getenv("MYSQL_HOST");
	if (!tmp) {
		fprintf(stderr, "MYSQL_HOST is not set\n");
		return -EINVAL;
	}
	cred->host = tmp;

	tmp = getenv("MYSQL_USER");
	if (!tmp) {
		fprintf(stderr, "MYSQL_USER is not set\n");
		return -EINVAL;
	}
	cred->user = tmp;

	tmp = getenv("MYSQL_PASS");
	if (!tmp) {
		fprintf(stderr, "MYSQL_PASS is not set\n");
		return -EINVAL;
	}
	cred->pass = tmp;

	tmp = getenv("MYSQL_DB");
	if (!tmp) {
		fprintf(stderr, "MYSQL_DB is not set\n");
		return -EINVAL;
	}
	cred->db = tmp;

	tmp = getenv("MYSQL_TABLE");
	if (!tmp) {
		fprintf(stderr, "MYSQL_TABLE is not set\n");
		return -EINVAL;
	}
	wrk->table_name = tmp;

	tmp = getenv("MYSQL_PORT");
	if (!tmp) {
		cred->port = 3306;
		return 0;
	}

	port = atoi(tmp);
	if (port < 1 || port > 65535) {
		fprintf(stderr, "Invalid port: %d\n", port);
		return -EINVAL;
	}

	cred->port = (uint16_t) port;
	return 0;
}

static int sql_worker_thread_init(struct mysql_wrk *wrk)
{
	wrk->cb = NULL;
	wrk->arg = NULL;
	wrk->is_used = false;

	wrk->conn = mysql_init(NULL);
	if (!wrk->conn) {
		g_stop = true;
		fprintf(stderr, "Failed to initialize mysql\n");
		return -ENOMEM;
	}

	if (!mysql_real_connect(wrk->conn, mysql_cred.host, mysql_cred.user, mysql_cred.pass,
				mysql_cred.db, mysql_cred.port, NULL, 0)) {
		fprintf(stderr, "Failed to connect to mysql: %s\n", mysql_error(wrk->conn));
		mysql_close(wrk->conn);
		return -EINVAL;
	}

	return 0;
}

static void *sql_worker_func(void *arg)
{
	struct mysql_wrk *wrk = arg;
	struct mysql_wrk_pool *pool = wrk->pool;
	int ret;

	pthread_mutex_lock(&pool->lock);
	ret = sql_worker_thread_init(wrk);
	pthread_mutex_unlock(&pool->lock);
	if (ret) {
		g_stop = true;
		return NULL;
	}

	atomic_fetch_add(&pool->nr_ready_wrk, 1);
	printf("MySQL worker %04u is ready\n", wrk->tid);

	pthread_mutex_lock(&pool->lock);
	while (!g_stop) {
		if (!wrk->is_used) {
			pthread_cond_wait(&pool->cond, &pool->lock);
			continue;
		}
		pthread_mutex_unlock(&pool->lock);

		wrk->cb(wrk, wrk->arg);

		pthread_mutex_lock(&pool->lock);
		wrk->is_used = false;
		wrk->arg = NULL;
		wrk->cb = NULL;
	}

	pthread_mutex_unlock(&pool->lock);
	mysql_close(wrk->conn);
	mysql_thread_end();
	printf("MySQL worker %04u is exiting...\n", wrk->tid);
	return NULL;
}

static void sigaction_handler(int sig)
{
	char c = '\n';
	g_stop = true;
	sig = write(STDOUT_FILENO, &c, 1);
	if (sig < 0)
		exit(-EINVAL);
}

static int setup_sigaction(void)
{
	struct sigaction act = { .sa_handler = sigaction_handler };
	int ret;

	ret = sigaction(SIGINT, &act, NULL);
	if (ret)
		goto err;
	ret = sigaction(SIGTERM, &act, NULL);
	if (ret)
		goto err;
	act.sa_handler = SIG_IGN;
	ret = sigaction(SIGPIPE, &act, NULL);
	if (ret)
		goto err;

	return 0;

err:
	ret = errno;
	perror("sigaction");
	return ret;
}

static int start_mysql_workers(struct mysql_wrk_pool *pool, size_t nr_workers)
{
	struct mysql_wrk *wrk;
	uint32_t i;
	int ret;

	pool->wrk = calloc(nr_workers, sizeof(*pool->wrk));
	if (!pool->wrk) {
		fprintf(stderr, "Failed to allocate mysql workers\n");
		return -ENOMEM;
	}

	pool->nr_wrk = nr_workers;
	ret = pthread_mutex_init(&pool->lock, NULL);
	if (ret) {
		fprintf(stderr, "Failed to initialize mysql worker mutex: %s\n", strerror(ret));
		goto out_free_wrk;
	}

	ret = pthread_cond_init(&pool->cond, NULL);
	if (ret) {
		fprintf(stderr, "Failed to initialize mysql worker cond: %s\n", strerror(ret));
		goto out_free_mutex;
	}

	for (i = 0; i < nr_workers; i++) {
		wrk = &pool->wrk[i];
		wrk->pool = pool;
		wrk->tid = i;
		ret = pthread_create(&wrk->thread, NULL, &sql_worker_func, wrk);
		if (ret) {
			fprintf(stderr, "Failed to create mysql worker thread: %s\n", strerror(ret));
			goto out_free_threads;
		}
	}

	return 0;

out_free_threads:
	pthread_mutex_lock(&pool->lock);
	g_stop = true;
	pthread_cond_broadcast(&pool->cond);
	pthread_mutex_unlock(&pool->lock);

	printf("Waiting for mysql workers to exit...\n");
	while (i--) {
		wrk = &pool->wrk[i];
		pthread_join(wrk->thread, NULL);
	}
	pthread_cond_destroy(&pool->cond);

out_free_mutex:
	pthread_mutex_destroy(&pool->lock);
out_free_wrk:
	free(pool->wrk);
	return ret;
}

static void mode_dml_exit(struct worker *wrk)
{
	struct mysql_wrk_pool *pool = &wrk->mysql_pool;
	struct mysql_wrk *wrk_tmp;
	uint32_t i = pool->nr_wrk;

	pthread_mutex_lock(&pool->lock);
	g_stop = true;
	pthread_cond_broadcast(&pool->cond);
	pthread_mutex_unlock(&pool->lock);

	printf("Waiting for %u mysql worker(s) to exit...\n", i);
	while (i--) {
		wrk_tmp = &pool->wrk[i];
		pthread_join(wrk_tmp->thread, NULL);
	}
	pthread_cond_destroy(&pool->cond);
	pthread_mutex_destroy(&pool->lock);
	free(pool->wrk);
}

int main(int argc, char **argv)
{
	struct worker worker;
	void *err;
	int ret;

	if (argc != 3) {
		fprintf(stderr, "Usage: %s [dml|ddl] [target_file]\n", argv[0]);
		return -EINVAL;
	}

	memset(&worker, 0, sizeof(worker));

	if (!strcmp(argv[1], "ddl")) {
		run_mode = MODE_DDL;
	} else if (!strcmp(argv[1], "dml")) {
		ret = parse_mysql_env(&mysql_cred, &worker);
		if (ret) {
			fprintf(stderr, "Failed to parse mysql env\n");
			return ret;
		}

		ret = setup_sigaction();
		if (ret) {
			fprintf(stderr, "Failed to setup sigaction\n");
			return ret;
		}

		ret = pthread_mutex_init(&worker.pending_wrk_lock, NULL);
		if (ret) {
			fprintf(stderr, "Failed to initialize pending work mutex: %s\n", strerror(ret));
			return ret;
		}

		ret = pthread_cond_init(&worker.pending_wrk_cond, NULL);
		if (ret) {
			pthread_mutex_destroy(&worker.pending_wrk_lock);
			fprintf(stderr, "Failed to initialize pending work cond: %s\n", strerror(ret));
			return ret;
		}

		ret = start_mysql_workers(&worker.mysql_pool, NR_MYSQL_WORKERS);
		if (ret) {
			fprintf(stderr, "Failed to start mysql workers\n");
			return ret;
		}

		run_mode = MODE_DML;
	} else {
		fprintf(stderr, "Invalid command: %s\n", argv[1]);
		return -EINVAL;
	}

	worker.fp = fopen(argv[2], "rb");
	if (!worker.fp) {
		ret = errno;
		fprintf(stderr, "Failed to open file: %s (%s)\n", strerror(ret), argv[2]);
		return ret;
	}

	err = worker_func(&worker);

	if (run_mode == MODE_DML)
		mode_dml_exit(&worker);

	fclose(worker.fp);
	return -PTR_ERR(err);
}
