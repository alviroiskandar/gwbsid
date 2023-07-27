// SPDX-License-Identifier: GPL-2.0-only
/*
 * Copyright (C) 2023  Alviro Iskandar Setiawan <alviro.iskandar@gnuweeb.org>
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE	200809L
#endif

#include <json-c/json.h>
#include <mysql/mysql.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <stdatomic.h>
#include <arpa/inet.h>
#include <assert.h>
#include <signal.h>
#include <pthread.h>
#include <stdbool.h>
#include <sys/eventfd.h>
#include <time.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <limits.h>

#define TCP_LISTEN_BACKLOG	1024
#define MYSQL_POOL_SIZE		128
#define NR_WORKERS		8
#define NR_WORK_ITEMS		1024
#define NR_EPOLL_EVENTS		32
#define NR_CLIENTS		1024
#define NR_FORK_WORKERS		1
#define NR_IO_WORKERS		32
#define ARRAY_SIZE(X)		(sizeof(X) / sizeof((X)[0]))

enum {
	TASK_COMM_LEN = 16,
};

struct stack {
	uint32_t	*data;
	uint32_t	size;
	uint32_t	top;
	pthread_mutex_t	lock;
};

struct mysql_conn {
	MYSQL		*mysql;
	bool		is_used;
	bool		is_connected;
};

struct mysql_pool {
	struct mysql_conn	*conn_arr;
	struct stack		stack;
};

struct mysql_cred {
	char	*host;
	char	*user;
	char	*pass;
	char	*db;
	uint16_t port;
};

struct tcp_bind_cfg {
	const char	*host;
	uint16_t	port;
};

struct context;

struct http_hdr_field {
	const char	*key;
	const char	*val;
};

struct http_req_header {
	char			*method;
	char			*uri;
	char			*qs;
	char			*version;
	struct http_hdr_field	*fields;
	uint32_t		nr_fields;
};

struct http_res_header {
	int			code;
	struct http_hdr_field	*fields;
	uint32_t		nr_fields;
};

struct http_res_body {
	char		*buf;
	uint32_t	len;
	uint64_t	off;
	int		fd;
	struct stat	st;
};

struct http_response {
	struct http_res_header	header;
	struct http_res_body	body;
	char			*hdr_buf;
	uint32_t		hdr_buf_len;
	uint32_t		hdr_buf_off;
};

struct client {
	int			fd;
	struct sockaddr_storage	addr;
	char			*buf;
	struct mysql_conn	*db_conn;
	struct http_req_header	header;
	struct http_response	res;
	uint32_t		buf_len;
	uint32_t		buf_size;
	uint32_t		id;
	struct timespec		last_active;
	struct cancel_point	*cancel;
	bool			pollout;
	bool			got_header;
	_Atomic(int)		refcnt;
};

struct client_slot {
	struct client		*clients;
	struct stack		 stack;
};

struct worker {
	int			epoll_fd;
	int			event_fd;
	int			timeout;
	uint64_t		event_fd_val;
	bool			kill_current;
	volatile bool		is_sleeping;
	struct context		*ctx;
	struct client_slot	client_slot;
	pthread_t		thread;
	uint32_t		id;
	struct epoll_event	events[NR_EPOLL_EVENTS];
	uint32_t		nr_events;
};

struct work_item {
	void	*data;
	void	(*callback)(void *data);
	void	(*free)(void *data);
};

struct work_queue {
	pthread_mutex_t		lock;
	pthread_cond_t		cond;
	struct work_item	*items;
	uint32_t		mask;
	uint32_t		head;
	uint32_t		tail;
};

struct io_worker {
	pthread_t		thread;
	struct context		*ctx;
	uint32_t		id;
};

struct context {
	volatile bool		stop;
	int			tcp_fd;
	struct mysql_pool	mysql_pool;
	struct worker		*workers;
	struct work_queue	work_queue;
	struct io_worker	*io_workers;
	uint32_t		nr_io_workers;
	uint32_t		nr_workers;
	struct tcp_bind_cfg	tcp_bind_cfg;
	struct mysql_cred	mysql_cred;
	_Atomic(uint32_t)	online_workers;
};

static struct context *g_ctx;


static inline void *ERR_PTR(long err)
{
	return (void *)err;
}

static inline long PTR_ERR(const void *ptr)
{
	return (long)ptr;
}

static inline bool IS_ERR(const void *ptr)
{
	return (unsigned long)ptr > (unsigned long)-4096ul;
}

static void sigaction_handler(int sig)
{
	char c = '\n';

	if (!g_ctx)
		return;

	if (g_ctx->stop)
		return;

	g_ctx->stop = true;
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
	return -ret;
}

static int client_sub_refcnt(struct client *client, int sub)
{
	return atomic_fetch_sub(&client->refcnt, sub);
}

static int client_add_refcnt(struct client *client, int add)
{
	return atomic_fetch_add(&client->refcnt, add);
}

static int client_get_refcnt(struct client *client)
{
	return atomic_load(&client->refcnt);
}

static int parse_mysql_env(struct mysql_cred *cred)
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

	cred->port = (uint16_t)port;
	return 0;
}

static int parse_tcp_bind_cfg(struct tcp_bind_cfg *cfg)
{
	char *tmp;

	cfg->host = "::";
	cfg->port = 8444;

	tmp = getenv("TCP_BIND_HOST");
	if (tmp)
		cfg->host = tmp;

	tmp = getenv("TCP_BIND_PORT");
	if (tmp) {
		int port = atoi(tmp);
		if (port < 1 || port > 65535) {
			fprintf(stderr, "Invalid port: %d\n", port);
			return -EINVAL;
		}
		cfg->port = (uint16_t)port;
	}

	return 0;
}

static int fill_sockaddr_ss(struct sockaddr_storage *ss, const char *addr, uint16_t port)
{
	struct sockaddr_in6 *sin6 = (void *)ss;
	struct sockaddr_in *sin = (void *)ss;
	int err;

	memset(ss, 0, sizeof(*ss));

	err = inet_pton(AF_INET6, addr, &sin6->sin6_addr);
	if (err == 1) {
		sin6->sin6_family = AF_INET6;
		sin6->sin6_port = htons(port);
		return 0;
	}

	err = inet_pton(AF_INET, addr, &sin->sin_addr);
	if (err == 1) {
		sin->sin_family = AF_INET;
		sin->sin_port = htons(port);
		return 0;
	}

	fprintf(stderr, "Invalid bind address: %s\n", addr);
	return -EINVAL;
}

static socklen_t get_sockaddr_len(const struct sockaddr_storage *ss)
{
	switch (ss->ss_family) {
	case AF_INET:
		return sizeof(struct sockaddr_in);
	case AF_INET6:
		return sizeof(struct sockaddr_in6);
	default:
		return 0;
	}
}

static const char *get_str_ss(struct sockaddr_storage *ss)
{
	static __thread char __buf[8][INET6_ADDRSTRLEN + 8];
	static __thread uint8_t __idx;

	char *buf, *ret;
	void *addr_ptr;
	uint16_t port;
	int family;

	if (ss->ss_family != AF_INET6 && ss->ss_family != AF_INET)
		return "(unknown family)";

	ret = buf = __buf[__idx++ % 8];
	family = ss->ss_family;

	if (family == AF_INET6) {
		struct sockaddr_in6 *sin6 = (void *)ss;

		*buf++ = '[';
		addr_ptr = &sin6->sin6_addr;
		port = ntohs(sin6->sin6_port);
	} else {
		struct sockaddr_in *sin = (void *)ss;

		addr_ptr = &sin->sin_addr;
		port = ntohs(sin->sin_port);
	}

	inet_ntop(family, addr_ptr, buf, INET6_ADDRSTRLEN);

	if (family == AF_INET6) {
		buf += strlen(buf);
		*buf++ = ']';
	}

	sprintf(buf, ":%hu", port);
	return ret;
}

static int init_tcp_socket(struct context *ctx)
{
	struct sockaddr_storage addr;
	int fd, err, val;

	err = fill_sockaddr_ss(&addr, ctx->tcp_bind_cfg.host, ctx->tcp_bind_cfg.port);
	if (err)
		return err;

	fd = socket(addr.ss_family, SOCK_STREAM | SOCK_NONBLOCK, 0);
	if (fd < 0) {
		err = -errno;
		perror("socket()");
		return err;
	}

	val = 1;
	setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
	setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &val, sizeof(val));

	err = bind(fd, (struct sockaddr *)&addr, get_sockaddr_len(&addr));
	if (err < 0) {
		err = -errno;
		perror("bind");
		goto out_err;
	}

	err = listen(fd, TCP_LISTEN_BACKLOG);
	if (err < 0) {
		err = -errno;
		perror("listen");
		goto out_err;
	}

	printf("Listening on %s...\n", get_str_ss(&addr));
	ctx->tcp_fd = fd;
	return 0;

out_err:
	close(fd);
	return err;
}

static void destroy_tcp_socket(struct context *ctx)
{
	if (ctx->tcp_fd) {
		close(ctx->tcp_fd);
		ctx->tcp_fd = -1;
	}
}

static int __push_stack(struct stack *st, uint32_t val)
{
	uint32_t top;

	top = st->top;
	if (top >= st->size)
		return -EOVERFLOW;

	st->data[top] = val;
	st->top = top + 1;
	return 0;
}

static int __pop_stack(struct stack *st, uint32_t *val)
{
	uint32_t top;

	top = st->top;
	if (top == 0)
		return -ENOENT;

	top--;
	st->top = top;
	*val = st->data[top];
	return 0;
}

static int push_stack(struct stack *st, uint32_t val)
{
	int ret;

	pthread_mutex_lock(&st->lock);
	ret = __push_stack(st, val);
	pthread_mutex_unlock(&st->lock);
	return ret;
}

static uint32_t count_stack(struct stack *st)
{
	uint32_t ret;

	pthread_mutex_lock(&st->lock);
	ret = st->top;
	pthread_mutex_unlock(&st->lock);
	return ret;
}

static int pop_stack(struct stack *st, uint32_t *val)
{
	int ret;

	pthread_mutex_lock(&st->lock);
	ret = __pop_stack(st, val);
	pthread_mutex_unlock(&st->lock);
	return ret;
}

static int init_stack(struct stack *st, uint32_t size)
{
	uint32_t *data;
	int ret;

	ret = pthread_mutex_init(&st->lock, NULL);
	if (ret) {
		errno = ret;
		perror("pthread_mutex_init() in init_stack()");
		return -ret;
	}

	data = calloc(size, sizeof(*data));
	if (!data) {
		errno = ENOMEM;
		perror("calloc() in init_stack()");
		return -ENOMEM;
	}

	st->data = data;
	st->size = size;
	st->top = 0;
	return 0;
}

static void destroy_stack(struct stack *st)
{
	if (st->data) {
		free(st->data);
		pthread_mutex_destroy(&st->lock);
		memset(st, 0, sizeof(*st));
	}
}

static int init_mysql_pool(struct context *ctx)
{
	uint32_t i = MYSQL_POOL_SIZE;
	struct mysql_conn *conn;
	int ret;

	conn = calloc(i, sizeof(*conn));
	if (!conn) {
		errno = ENOMEM;
		perror("calloc() in init_mysql_pool()");
		return -ENOMEM;
	}

	ret = init_stack(&ctx->mysql_pool.stack, i);
	if (ret) {
		free(conn);
		return ret;
	}

	while (i--) {
		ret = push_stack(&ctx->mysql_pool.stack, i);
		assert(ret == 0);
	}

	ctx->mysql_pool.conn_arr = conn;
	return 0;
}

static int __push_work_queue(struct work_queue *queue, struct work_item *item)
{
	uint32_t tail;

	tail = queue->tail;
	if ((tail - queue->head) >= queue->mask)
		return -EAGAIN;

	queue->items[tail & queue->mask] = *item;
	queue->tail = tail + 1;
	return 0;
}

static int __pop_work_queue(struct work_queue *queue, struct work_item *item)
{
	uint32_t head;
	uint32_t idx;

	head = queue->head;
	if (head == queue->tail)
		return -EAGAIN;

	idx = head & queue->mask;
	*item = queue->items[idx];
	queue->items[idx].callback = NULL;
	queue->items[idx].data = NULL;
	queue->items[idx].free = NULL;
	queue->head = head + 1;
	return 0;
}

static int push_work_queue(struct work_queue *queue, struct work_item *item)
{
	int ret;

	pthread_mutex_lock(&queue->lock);
	ret = __push_work_queue(queue, item);
	pthread_mutex_unlock(&queue->lock);
	pthread_cond_signal(&queue->cond);
	return ret;
}

static int pop_work_queue(struct work_queue *queue, struct work_item *item)
{
	int ret;

	pthread_mutex_lock(&queue->lock);
	ret = __pop_work_queue(queue, item);
	pthread_mutex_unlock(&queue->lock);
	return ret;
}

static int schedule_work(struct worker *wrk, void (*callback)(void *arg),
			 void *arg, void (*free_func)(void *arg))
{
	struct work_item item = {
		.callback = callback,
		.data = arg,
		.free = free_func,
	};

	return push_work_queue(&wrk->ctx->work_queue, &item);
}

static void set_thread_name(pthread_t t, const char *fmt, ...)
{
	char name[TASK_COMM_LEN];
	va_list ap;

	va_start(ap, fmt);
	vsnprintf(name, sizeof(name), fmt, ap);
	va_end(ap);

	pthread_setname_np(t, name);
}

static void *run_io_worker(void *arg)
{
	struct io_worker *worker = arg;
	struct context *ctx = worker->ctx;
	struct work_queue *wq = &ctx->work_queue;
	int ret;

	pthread_mutex_lock(&wq->lock);
	while (!ctx->stop) {
		struct work_item item;

		ret = __pop_work_queue(wq, &item);
		if (ret < 0) {
			pthread_cond_wait(&wq->cond, &wq->lock);
			continue;
		}

		pthread_mutex_unlock(&wq->lock);

		if (item.callback)
			item.callback(item.data);

		if (item.free)
			item.free(item.data);

		pthread_mutex_lock(&wq->lock);
	}
	pthread_mutex_unlock(&wq->lock);

	return NULL;
}

static int init_io_workers(struct context *ctx)
{
	struct io_worker *workers;
	uint32_t i;
	int ret;

	ctx->nr_io_workers = NR_IO_WORKERS;
	workers = calloc(ctx->nr_io_workers, sizeof(*workers));
	if (!workers) {
		errno = ENOMEM;
		perror("calloc() in init_io_workers()");
		return -ENOMEM;
	}

	for (i = 0; i < ctx->nr_io_workers; i++) {
		workers[i].id = i;
		workers[i].ctx = ctx;
		ret = pthread_create(&workers[i].thread, NULL, run_io_worker, &workers[i]);
		if (ret) {
			errno = ret;
			perror("pthread_create() in init_io_workers()");
			goto out_err;
		}

		set_thread_name(workers[i].thread, "io-wrk-%u", i);
	}

	ctx->io_workers = workers;
	return 0;

out_err:
	pthread_mutex_lock(&ctx->work_queue.lock);
	ctx->stop = true;
	pthread_cond_broadcast(&ctx->work_queue.cond);
	pthread_mutex_unlock(&ctx->work_queue.lock);

	while (i--) {
		pthread_kill(workers[i].thread, SIGTERM);
		pthread_join(workers[i].thread, NULL);
	}
	free(workers);
	return -ret;
}

static int do_mysql_connect(struct worker *wrk, struct mysql_conn *conn)
{
	conn->mysql = mysql_init(NULL);
	if (!conn->mysql) {
		fprintf(stderr, "Warning: mysql_init() failed in do_mysql_connect() (wrk=%u)\n",
			wrk->id);
		return -ENOMEM;
	}

	if (!mysql_real_connect(conn->mysql, wrk->ctx->mysql_cred.host,
				wrk->ctx->mysql_cred.user,
				wrk->ctx->mysql_cred.pass,
				wrk->ctx->mysql_cred.db,
				wrk->ctx->mysql_cred.port,
				NULL, 0)) {
		fprintf(stderr, "Warning: mysql_real_connect() failed in do_mysql_connect() (wrk=%u)\n",
			wrk->id);
		mysql_close(conn->mysql);
		conn->mysql = NULL;
		return -EIO;
	}

	conn->is_connected = true;
	return 0;
}

static void put_mysql_conn(struct worker *wrk, struct mysql_conn *conn)
{
	struct mysql_pool *pool = &wrk->ctx->mysql_pool;
	int err;

	assert(conn->is_used);

	mysql_close(conn->mysql);
	conn->mysql = NULL;
	conn->is_connected = false;
	conn->is_used = false;
	err = push_stack(&pool->stack, conn - pool->conn_arr);
	if (err) {
		fprintf(stderr, "Warning: push_stack() failed in put_mysql_conn() (wrk=%u)\n",
			wrk->id);
	}
}

static struct mysql_conn *get_mysql_conn(struct worker *wrk)
{
	struct mysql_pool *pool = &wrk->ctx->mysql_pool;
	struct mysql_conn *conn;
	uint32_t idx;
	int err;

	err = pop_stack(&pool->stack, &idx);
	if (err)
		return ERR_PTR(-EAGAIN);

	conn = &pool->conn_arr[idx];
	assert(!conn->is_used);
	conn->is_used = true;

	if (!conn->is_connected) {
		err = do_mysql_connect(wrk, conn);
		if (err) {
			put_mysql_conn(wrk, conn);
			return ERR_PTR(err);
		}
	}

	return conn;
}

static int init_work_queue(struct context *ctx)
{
	static const uint32_t nr_items = NR_WORK_ITEMS;
	struct work_queue *queue = &ctx->work_queue;
	struct work_item *items;
	uint32_t nr;
	int err;

	nr = 1u;
	while (nr < nr_items)
		nr <<= 1;

	items = calloc(nr, sizeof(*items));
	if (!items) {
		errno = ENOMEM;
		perror("calloc() in init_work_queue()");
		return -ENOMEM;
	}

	err = pthread_mutex_init(&queue->lock, NULL);
	if (err) {
		errno = err;
		perror("pthread_mutex_init() in init_work_queue()");
		free(items);
		return -err;
	}

	err = pthread_cond_init(&queue->cond, NULL);
	if (err) {
		errno = err;
		perror("pthread_cond_init() in init_work_queue()");
		pthread_mutex_destroy(&queue->lock);
		free(items);
		return -err;
	}

	queue->items = items;
	queue->mask = nr - 1;
	queue->head = 0;
	queue->tail = 0;
	return 0;
}

static void destroy_work_queue(struct context *ctx)
{
	struct work_queue *queue = &ctx->work_queue;
	size_t i;

	if (!queue->items)
		return;

	for (i = 0; i <= queue->mask; i++) {
		struct work_item *item = &queue->items[i];

		if (item->free)
			item->free(item->data);
	}

	free(queue->items);
	pthread_mutex_destroy(&queue->lock);
	pthread_cond_destroy(&queue->cond);
	memset(queue, 0, sizeof(*queue));
}

static void destroy_mysql_pool(struct context *ctx)
{
	struct mysql_pool *pool = &ctx->mysql_pool;
	struct mysql_conn *conn;
	uint32_t i;

	if (!pool->conn_arr)
		return;

	for (i = 0; i < pool->stack.size; i++) {
		conn = &pool->conn_arr[i];
		if (conn->mysql) {
			mysql_close(conn->mysql);
			conn->mysql = NULL;
		}
	}

	free(pool->conn_arr);
	destroy_stack(&pool->stack);
	memset(pool, 0, sizeof(*pool));
}

static void *run_worker(void *arg);

static int init_epoll_for_worker(struct worker *wrk)
{
	struct context *ctx = wrk->ctx;
	struct epoll_event ev;
	int evfd, fd, err;

	fd = epoll_create(10000);
	if (fd < 0) {
		fd = -errno;
		perror("epoll_create() in init_epoll_for_worker()");
		return fd;
	}

	evfd = eventfd(0, EFD_NONBLOCK);
	if (evfd < 0) {
		evfd = -errno;
		perror("eventfd() in init_epoll_for_worker()");
		close(fd);
		return evfd;
	}

	ev.events = EPOLLIN | EPOLLPRI;
	ev.data.u64 = 1;
	err = epoll_ctl(fd, EPOLL_CTL_ADD, evfd, &ev);
	if (err < 0) {
		err = -errno;
		goto out_err_epl_ctl;
	}

	wrk->epoll_fd = fd;
	wrk->event_fd = evfd;

	/*
	 * Only the first worker that monitors the main TCP socket.
	 */
	if (wrk->id > 0)
		return 0;

	ev.events = EPOLLIN | EPOLLPRI;
	ev.data.u64 = 0;
	err = epoll_ctl(fd, EPOLL_CTL_ADD, ctx->tcp_fd, &ev);
	if (err < 0) {
		err = -errno;
		goto out_err_epl_ctl;
	}

	return 0;

out_err_epl_ctl:
	perror("epoll_ctl(EPOLL_CTL_ADD) in init_epoll_for_worker()");
	close(fd);
	close(evfd);
	wrk->epoll_fd = wrk->event_fd = -1;
	return err;
}

static void reset_client(struct client *client)
{
	struct http_res_header *res_header = &client->res.header;
	struct http_hdr_field *fields = res_header->fields;
	struct http_res_body *res_body = &client->res.body;
	size_t i;

	if (res_body->fd >= 0)
		close(res_body->fd);

	for (i = 0; i < res_header->nr_fields; i++) {
		free((void *)fields[i].key);
		free((void *)fields[i].val);
	}

	if (client->fd >= 0) {
		printf("Closing a client connection (id=%u; addr=%s)\n", client->id,
		       get_str_ss(&client->addr));
		close(client->fd);
		client->fd = -1;
	}

	if (client->header.fields) {
		free(client->header.fields);
		memset(&client->header, 0, sizeof(client->header));
	}

	if (client->buf) {
		free(client->buf);
		client->buf = NULL;
		client->buf_size = 0;
	}

	if (client->res.body.buf)
		free(client->res.body.buf);

	if (client->res.header.fields)
		free(client->res.header.fields);

	if (client->res.hdr_buf)
		free(client->res.hdr_buf);

	memset(&client->res, 0, sizeof(client->res));
	client->buf_len = 0;
	client->pollout = false;
	client->got_header = false;
	res_body->fd = -1;
}

static int init_client_slot(struct client_slot *cs)
{
	uint32_t i = NR_CLIENTS;
	struct client *clients;
	int ret;

	clients = calloc(i, sizeof(*clients));
	if (!clients) {
		errno = ENOMEM;
		perror("calloc() in init_client_slot()");
		return -ENOMEM;
	}

	ret = init_stack(&cs->stack, i);
	if (ret) {
		free(clients);
		return ret;
	}

	while (i--) {
		clients[i].id = i;
		clients[i].fd = -1;
		clients[i].res.body.fd = -1;
		ret = push_stack(&cs->stack, i);
		assert(ret == 0);
	}

	cs->clients = clients;
	return 0;
}

static void destroy_client_slot(struct client_slot *cs)
{
	struct client *clients = cs->clients;
	uint32_t i;

	if (!clients)
		return;

	pthread_mutex_lock(&cs->stack.lock);
	for (i = 0; i < cs->stack.size; i++)
		reset_client(&clients[i]);
	pthread_mutex_unlock(&cs->stack.lock);

	free(clients);
	destroy_stack(&cs->stack);
	memset(cs, 0, sizeof(*cs));
}

static bool del_client_from_epoll(struct worker *wrk, struct client *client)
{
	int err;

	err = epoll_ctl(wrk->epoll_fd, EPOLL_CTL_DEL, client->fd, NULL);
	if (err < 0) {
		err = -errno;
		perror("epoll_ctl(EPOLL_CTL_DEL) in del_client_from_epoll()");
		return false;
	}

	return true;
}

static bool put_client_slot(struct worker *wrk, struct client *client)
{
	int err;

	err = client_sub_refcnt(client, 1);
	assert(err > 0);
	if (err > 1)
		return false;

	if (client->db_conn) {
		put_mysql_conn(wrk, client->db_conn);
		client->db_conn = NULL;
	}

	reset_client(client);
	err = push_stack(&wrk->client_slot.stack, client->id);
	if (err) {
		fprintf(stderr, "Warning: push_stack() failed in put_client_slot() (wrk=%u; id=%u)\n",
			wrk->id, client->id);
	}

	return true;
}

static struct client *get_client_slot(struct worker *wrk)
{
	struct client *ret;
	uint32_t idx;
	int err;

	err = pop_stack(&wrk->client_slot.stack, &idx);
	if (err)
		return ERR_PTR(-EAGAIN);

	ret = &wrk->client_slot.clients[idx];
	assert(ret->fd == -1);
	assert(ret->id == idx);
	assert(ret->buf_len == 0);

	if (!ret->buf) {
		ret->buf = malloc(4096);
		if (!ret->buf) {
			fprintf(stderr, "Warning: malloc() failed in get_client_slot()\n");
			put_client_slot(wrk, ret);
			return ERR_PTR(-ENOMEM);
		}
		ret->buf_size = 4096;
	}

	err = atomic_fetch_add_explicit(&ret->refcnt, 1, memory_order_relaxed);
	assert(err == 0);
	return ret;
}

static int init_workers(struct context *ctx)
{
	struct worker *wrk;
	uint32_t i;
	int ret;

	ctx->nr_workers = NR_WORKERS;

	wrk = calloc(ctx->nr_workers, sizeof(*wrk));
	if (!wrk) {
		errno = ENOMEM;
		perror("calloc() in init_workers()");
		return -ENOMEM;
	}

	for (i = 0; i < ctx->nr_workers; i++) {
		wrk[i].ctx = ctx;
		wrk[i].nr_events = NR_EPOLL_EVENTS;
		wrk[i].timeout = 10000;
		wrk[i].id = i;

		ret = init_client_slot(&wrk[i].client_slot);
		if (ret)
			goto out_err;

		ret = init_epoll_for_worker(&wrk[i]);
		if (ret)
			goto out_err;

		/*
		 * Skip spawning thread for the first worker.
		 * The main thread will be used as the first worker.
		 */
		if (i == 0)
			continue;

		ret = pthread_create(&wrk[i].thread, NULL, run_worker, &wrk[i]);
		if (ret) {
			errno = ret;
			perror("pthread_create() in init_workers()");
			goto out_err;
		}

		set_thread_name(wrk[i].thread, "ewrk-%u", i);
	}

	ctx->workers = wrk;
	return 0;

out_err:
	pthread_mutex_lock(&ctx->work_queue.lock);
	ctx->stop = true;
	pthread_cond_broadcast(&ctx->work_queue.cond);
	pthread_mutex_unlock(&ctx->work_queue.lock);

	while (i--) {
		pthread_kill(wrk[i].thread, SIGTERM);
		pthread_join(wrk[i].thread, NULL);
		destroy_client_slot(&wrk[i].client_slot);
		if (wrk[i].epoll_fd >= 0)
			close(wrk[i].epoll_fd);
		if (wrk[i].event_fd >= 0)
			close(wrk[i].event_fd);
	}

	free(wrk);
	return -ret;
}

static int poll_events(struct worker *wrk)
{
	struct epoll_event *ev = wrk->events;
	int nr;

	nr = epoll_wait(wrk->epoll_fd, ev, wrk->nr_events, wrk->timeout);
	if (nr < 0) {
		if (errno == EINTR)
			return 0;

		errno = -nr;
		perror("epoll_wait() in poll_events()");
		return -nr;
	}

	return nr;
}

static int do_accept(int tcp_fd, struct sockaddr_storage *ss, socklen_t *len,
		     bool *got_client)
{
	int err, fd;

	fd = accept4(tcp_fd, (struct sockaddr *)ss, len, SOCK_NONBLOCK);
	if (fd >= 0) {
		*got_client = true;
		return fd;
	}

	*got_client = false;
	err = -errno;
	if (err == -EAGAIN)
		return 0;

	if (err == -EMFILE || err == -ENFILE) {
		/*
		 * FIXME: This is a busy loop. We should use
		 *        use the resource more efficiently.
		 */
		fprintf(stderr, "Warning: accept4(): Too many open files (busy loop)\n");
		return 0;
	}

	perror("accept4() in accept_tcp_socket()");
	return err;
}

static struct worker *pick_best_worker(struct context *ctx)
{
	struct worker *ret;
	uint32_t i;

	for (i = 1; i < ctx->nr_workers; i++) {
		ret = &ctx->workers[i];
		if (count_stack(&ret->client_slot.stack) > 0)
			return ret;
	}

	return &ctx->workers[0];
}

static int notify_worker(struct worker *wrk)
{
	uint64_t val = 1;
	ssize_t wr_ret;
	int ret;

	wr_ret = write(wrk->event_fd, &val, sizeof(val));
	if (wr_ret < 0) {
		ret = -errno;
		perror("write() in install_client_to_worker()");
		return ret;
	}

	return 0;
}

static int install_client_to_worker(struct worker *wrk, struct worker *target_wrk,
				    struct client *cl)
{
	struct epoll_event ev;
	int ret;

	ev.events = EPOLLIN | EPOLLPRI;
	ev.data.ptr = cl;
	ret = epoll_ctl(target_wrk->epoll_fd, EPOLL_CTL_ADD, cl->fd, &ev);
	if (ret < 0) {
		ret = -errno;
		perror("epoll_ctl(EPOLL_CTL_ADD) in install_client_to_worker()");
		return ret;
	}

	if (wrk == target_wrk)
		return 0;

	return notify_worker(target_wrk);
}

static int accept_tcp_client(struct worker *wrk, int fd, struct sockaddr_storage *addr)
{
	struct worker *target_wrk;
	struct client *cl;
	int ret;

	target_wrk = pick_best_worker(wrk->ctx);
	cl = get_client_slot(target_wrk);
	if (IS_ERR(cl)) {
		if (PTR_ERR(cl) == -EAGAIN) {
			fprintf(stderr,
				"Warning: Client slot is full (dropping a new connection from %s)\n",
				get_str_ss(addr));
		}
		close(fd);
		return 0;
	}

	cl->fd = fd;
	cl->addr = *addr;

	ret = install_client_to_worker(wrk, target_wrk, cl);
	if (ret) {
		put_client_slot(target_wrk, cl);
		return ret;
	}

	printf("Accepted a new client %s (wrk=%u; id=%u)\n", get_str_ss(addr),
		target_wrk->id, cl->id);
	return 0;
}

static int accept_tcp_socket(struct worker *wrk)
{
	struct sockaddr_storage addr;
	bool got_client;
	socklen_t len;
	int ret;

	len = sizeof(addr);
	ret = do_accept(wrk->ctx->tcp_fd, &addr, &len, &got_client);
	if (ret < 0) {
		assert(!got_client);
		return ret;
	}

	if (!got_client)
		return 0;

	if (len > sizeof(addr)) {
		close(ret);
		fprintf(stderr, "Warning: accept4(): Truncated address (len = %u), dropping client...\n",
			len);
		return 0;
	}

	return accept_tcp_client(wrk, ret, &addr);
}

static int consume_eventfd(struct worker *wrk)
{
	ssize_t rd_ret;
	uint64_t val;
	int ret;

	rd_ret = read(wrk->event_fd, &val, sizeof(val));
	if (rd_ret < 0) {
		ret = -errno;
		perror("read() in consume_eventfd()");
		return ret;
	}

	return 0;
}

static int do_recv(struct client *cl)
{
	uint32_t len;
	ssize_t ret;
	char *buf;

	buf = cl->buf + cl->buf_len;
	len = cl->buf_size - cl->buf_len - 1u;
	ret = recv(cl->fd, buf, len, MSG_DONTWAIT);
	if (ret < 0) {
		ret = -errno;
		if (ret == -EAGAIN)
			return 0;

		perror("recv() in do_recv()");
		return ret;
	}

	if (ret == 0)
		return -ECONNRESET;

	cl->buf_len += (uint32_t)ret;
	cl->buf[cl->buf_len] = '\0';
	return 0;
}

static int validate_method(const char *method)
{
	if (!strcmp(method, "GET"))
		return 0;

	return -EINVAL;
}

static int parse_first_http_header(struct client *cl, char **next)
{
	uint32_t len = cl->buf_len;
	char *ptr, *tmp, *end, *cr;
	char *buf = cl->buf;

	end = buf + len;

	cr = strchr(buf, '\r');
	*cr = '\0';

	/*
	 * Extract the method.
	 */
	ptr = strchr(buf, ' ');
	if (!ptr)
		return -EINVAL;
	ptr[0] = '\0';
	cl->header.method = buf;
	if (validate_method(cl->header.method) < 0)
		return -EINVAL;

	/*
	 * Extract the URI.
	 */
	ptr++;
	if (ptr >= end || ptr[0] != '/')
		return -EINVAL;
	cl->header.uri = ptr;

	/*
	 * Extract the query string (if any).
	 */
	tmp = strchr(ptr, '?');
	if (tmp) {
		if (tmp >= end)
			return -EINVAL;
		tmp[0] = '\0';
		cl->header.qs = ++tmp;
		ptr = tmp;
	}

	/*
	 * Last, make sure it's an HTTP request.
	 */
	tmp = strchr(ptr, ' ');
	if (!tmp)
		return -EINVAL;

	tmp[0] = '\0';
	if (tmp + 6 >= end)
		return -EINVAL;
	if (strncmp(tmp + 1, "HTTP/", 5))
		return -EINVAL;
	*cr = '\r';
	tmp += 6;
	if (tmp >= end)
		return -EINVAL;
	cl->header.version = tmp;

	while (tmp[0] != '\r') {
		if (tmp >= end)
			return -EINVAL;
		tmp++;
	}

	tmp++;
	if (tmp >= end || tmp[0] != '\n')
		return -EINVAL;

	*next = tmp + 1;
	return 0;
}

static char *strtolower(char *str)
{
	char *ret = str;

	while (str[0]) {
		str[0] = tolower(str[0]);
		str++;
	}

	return ret;
}

static int parse_http_header_fields(struct client *cl, char **next)
{
	struct http_req_header *hdr = &cl->header;
	char *ptr, *end;

	hdr->fields = NULL;
	end = cl->buf + cl->buf_len;
	ptr = *next;

	while (1) {
		struct http_hdr_field *fields;
		char *key, *val, *tmp, *tmp2;
		size_t nr_fields;

		tmp = strtok_r(ptr, "\r\n", &tmp2);
		if (!tmp)
			break;

		ptr = NULL;
		if (tmp >= end)
			return -EINVAL;

		if (tmp[0] == '\0')
			break;

		key = tmp;
		val = strchr(tmp, ':');
		if (!val)
			return -EINVAL;

		val[0] = '\0';
		val++;
		if (val >= end)
			return -EINVAL;

		while (val[0] == ' ')
			val++;

		if (val >= end)
			return -EINVAL;

		key = strtolower(key);
		nr_fields = hdr->nr_fields + 1u;
		fields = realloc(hdr->fields, nr_fields * sizeof(*fields));
		if (!fields) {
			free(hdr->fields);
			hdr->fields = NULL;
			return -ENOMEM;
		}

		hdr->fields = fields;
		hdr->fields[hdr->nr_fields].key = key;
		hdr->fields[hdr->nr_fields].val = val;
		hdr->nr_fields = nr_fields;
	}

	return 0;
}

static int htoi(char *s)
{
	int value;
	int c;

	c = ((unsigned char *)s)[0];
	if (isupper(c))
		c = tolower(c);
	value = (c >= '0' && c <= '9' ? c - '0' : c - 'a' + 10) * 16;

	c = ((unsigned char *)s)[1];
	if (isupper(c))
		c = tolower(c);
	value += c >= '0' && c <= '9' ? c - '0' : c - 'a' + 10;

	return (value);
}

static size_t url_decode(char *str, size_t len)
{
	char *dest = str;
	char *data = str;

	while (len--) {
		if (*data == '+') {
			*dest = ' ';
		} else if (*data == '%' && len >= 2 &&
			   isxdigit((int) *(data + 1)) &&
			   isxdigit((int) *(data + 2))) {
			*dest = (char) htoi(data + 1);
			data += 2;
			len -= 2;
		} else {
			*dest = *data;
		}
		data++;
		dest++;
	}
	*dest = '\0';
	return dest - str;
}

static int parse_http_headers(struct client *cl)
{
	uint32_t len = cl->buf_len;
	char *buf = cl->buf;
	char *next;
	int ret;

	if (cl->got_header)
		return 0;

	if (len < 4)
		return 0;

	if (!strstr(buf, "\r\n\r\n"))
		return 0;

	ret = parse_first_http_header(cl, &next);
	if (ret < 0)
		return ret;

	ret = parse_http_header_fields(cl, &next);
	if (ret < 0)
		return ret;

	cl->got_header = true;
	return 0;
}

static int http_res_code(struct client *cl, int code)
{
	cl->res.header.code = code;
	return 0;
}

static int http_res_add_hdr(struct client *cl, const char *key, const char *val, ...)
{
	struct http_response *res = &cl->res;
	struct http_res_header *hdr = &res->header;
	struct http_hdr_field *fields;
	char *vval, *vkey;
	size_t nr_fields;
	va_list ap;

	va_start(ap, val);
	if (vasprintf(&vval, val, ap) < 0) {
		va_end(ap);
		return -ENOMEM;
	}
	va_end(ap);

	vkey = strdup(key);
	if (!vkey) {
		free(vval);
		return -ENOMEM;
	}

	nr_fields = hdr->nr_fields + 1u;
	fields = realloc(hdr->fields, nr_fields * sizeof(*fields));
	if (!fields) {
		free(vkey);
		free(vval);
		free(hdr->fields);
		hdr->fields = NULL;
		return -ENOMEM;
	}

	hdr->fields = fields;
	hdr->fields[hdr->nr_fields].key = vkey;
	hdr->fields[hdr->nr_fields].val = vval;
	hdr->nr_fields = nr_fields;
	return 0;
}

static int http_res_add_body_file(struct client *cl, const char *file)
{
	struct http_response *res = &cl->res;
	struct http_res_body *res_body = &res->body;
	int fd, ret;

	fd = open(file, O_RDONLY);
	if (fd < 0) {
		ret = -errno;
		perror("open() in http_res_add_body_file()");
		return ret;
	}

	ret = fstat(fd, &res_body->st);
	if (ret < 0) {
		ret = -errno;
		perror("fstat() in http_res_add_body_file()");
		close(fd);
		return ret;
	}

	// Don't allow non-regular files.
	if (!S_ISREG(res_body->st.st_mode)) {
		close(fd);
		return -EINVAL;
	}

	res_body->fd = fd;
	res_body->off = 0;
	assert(res_body->buf == NULL);
	return 0;
}

static int http_res_add_body(struct client *cl, const char *body)
{
	struct http_response *res = &cl->res;
	struct http_res_body *res_body = &res->body;
	char *tmp;
	size_t len;

	assert(res_body->fd < 0);
	if (!body)
		return 0;

	len = strlen(body);
	tmp = realloc(res_body->buf, res_body->len + len + 1u);
	if (!tmp) {
		free(res_body->buf);
		res_body->buf = NULL;
		return -ENOMEM;
	}

	res_body->buf = tmp;
	memcpy(res_body->buf + res_body->len, body, len);
	res_body->len += len;
	res_body->buf[res_body->len] = '\0';
	return 0;
}

static const char *http_code_to_str(int code)
{
	switch (code) {
	case 200:
		return "OK";
	case 404:
		return "Not Found";
	default:
		return "";
	}
}

int http_construct_hdr_buf(struct client *cl)
{
	struct http_response *res = &cl->res;
	size_t i, len;
	char *buf;
	int tmp;

	len = (size_t)snprintf(NULL, 0,
		"HTTP/1.1 %d %s\r\n"
		"Connection: close\r\n",
		res->header.code, http_code_to_str(res->header.code));
	for (i = 0; i < res->header.nr_fields; i++) {
		len += strlen(res->header.fields[i].key);
		len += strlen(res->header.fields[i].val);
		len += sizeof(": ");
		len += sizeof("\r\n");
	}

	len += sizeof("\r\n");
	buf = malloc(len + 1u);
	if (!buf)
		return -ENOMEM;

	res->hdr_buf = buf;
	tmp = snprintf(buf, len,
			"HTTP/1.1 %d %s\r\n"
			"Connection: close\r\n",
			res->header.code,
			http_code_to_str(res->header.code));
	len -= tmp;
	buf += tmp;

	for (i = 0; i < res->header.nr_fields; i++) {
		tmp = snprintf(buf, len, "%s: %s\r\n", res->header.fields[i].key,
			       res->header.fields[i].val);
		len -= tmp;
		buf += tmp;
	}

	snprintf(buf, len, "\r\n");
	res->hdr_buf_len = strlen(res->hdr_buf);
	res->hdr_buf_off = 0;
	return 0;
}

static int do_send(struct client *cl, const void *buf, size_t len)
{
	int fd = cl->fd;
	ssize_t ret;

	ret = send(fd, buf, len, MSG_DONTWAIT);
	if (ret < 0) {
		ret = -errno;
		if (ret == -EAGAIN)
			return 0;

		perror("send() in do_send()");
		return ret;
	}

	if (ret == 0)
		return -ECONNRESET;

	return (int)ret;
}

static int http_wrk_reg_pollout(struct worker *wrk, struct client *cl)
{
	struct epoll_event ev;
	int ret;

	if (cl->pollout)
		return 0;

	ev.events = EPOLLOUT;
	ev.data.ptr = cl;
	ret = epoll_ctl(wrk->epoll_fd, EPOLL_CTL_MOD, cl->fd, &ev);
	if (ret < 0) {
		ret = -errno;
		perror("epoll_ctl(EPOLL_CTL_MOD) in http_wrk_reg_pollout()");
		return ret;
	}

	cl->pollout = true;
	return 0;
}

static int http_send_response_headers(struct worker *wrk, struct client *cl)
{
	uint32_t len;
	char *buf;
	int ret;

	len = cl->res.hdr_buf_len - cl->res.hdr_buf_off;
	buf = cl->res.hdr_buf + cl->res.hdr_buf_off;
	ret = do_send(cl, buf, len);
	if (ret < 0)
		return ret;

	cl->res.hdr_buf_off += (uint32_t)ret;
	if (cl->res.hdr_buf_off < cl->res.hdr_buf_len)
		return http_wrk_reg_pollout(wrk, cl);

	free(cl->res.hdr_buf);
	cl->res.hdr_buf = NULL;
	cl->res.hdr_buf_len = 0;
	cl->res.hdr_buf_off = 0;
	return 0;
}

static int send_res_body_inline(struct worker *wrk, struct client *cl)
{
	uint32_t len;
	char *buf;
	int ret;

	len = cl->res.body.len - cl->res.body.off;
	buf = cl->res.body.buf + cl->res.body.off;
	ret = do_send(cl, buf, len);
	if (ret < 0)
		return ret;

	cl->res.body.off += (uint32_t)ret;
	if (cl->res.body.off < cl->res.body.len)
		return http_wrk_reg_pollout(wrk, cl);

	free(cl->res.body.buf);
	cl->res.body.buf = NULL;
	cl->res.body.len = 0;
	cl->res.body.off = 0;
	return 0;
}

static int send_res_body_fd(struct worker *wrk, struct client *cl)
{
	char buf[4096];
	ssize_t ret;

	ret = pread64(cl->res.body.fd, buf, sizeof(buf), cl->res.body.off);
	if (ret < 0) {
		ret = -errno;
		perror("pread64() in send_res_body_fd()");
		return ret;
	}

	if (ret == 0)
		return -ECONNRESET;

	ret = do_send(cl, buf, (size_t)ret);
	if (ret < 0)
		return ret;

	cl->res.body.off += (uint32_t)ret;
	if (cl->res.body.off < (uint64_t)cl->res.body.st.st_size)
		return http_wrk_reg_pollout(wrk, cl);

	return -ECONNRESET;
}

static int http_send_response_body(struct worker *wrk, struct client *cl)
{
	if (cl->res.body.buf)
		return send_res_body_inline(wrk, cl);

	return send_res_body_fd(wrk, cl);
}

static int http_queue_response(struct worker *wrk, struct client *cl)
{
	int ret;

	if (cl->res.hdr_buf) {
		ret = http_send_response_headers(wrk, cl);
		if (ret < 0)
			return ret;
	}

	assert(!cl->res.hdr_buf);
	if (cl->res.body.buf || cl->res.body.fd >= 0) {
		ret = http_send_response_body(wrk, cl);
		if (ret < 0)
			return ret;
	}

	return 0;
}

static int http_res_commit(struct worker *wrk, struct client *cl)
{
	int ret;

	ret = http_construct_hdr_buf(cl);
	if (ret < 0)
		return ret;

	return http_queue_response(wrk, cl);
}

static int http_route_404(struct worker *wrk, struct client *cl)
{
	int ret = 0;

	http_res_code(cl, 404);
	ret |= http_res_add_hdr(cl, "Content-Type", "text/plain");
	ret |= http_res_add_hdr(cl, "Content-Length", "14");
	ret |= http_res_add_body(cl, "404 Not Found\n");
	if (ret)
		return -ENOMEM;

	return http_res_commit(wrk, cl);
}

static int http_route_index(struct worker *wrk, struct client *cl)
{
	unsigned long long len;
	int ret;

	http_res_code(cl, 200);
	ret = http_res_add_body_file(cl, "web_data/views/index.html");
	if (ret < 0)
		return ret;

	len = (unsigned long long)cl->res.body.st.st_size;
	ret |= http_res_add_hdr(cl, "Content-Type", "text/html");
	ret |= http_res_add_hdr(cl, "Content-Length", "%llu", len);
	if (ret)
		return -ENOMEM;

	return http_res_commit(wrk, cl);
}

static const char *get_file_mime_by_ext(const char *fname)
{
	char path[PATH_MAX];
	char *base;
	char *last;
	char *ptr;

	path[sizeof(path) - 1] = '\0';
	strncpy(path, fname, sizeof(path));
	path[strlen(fname)] = '\0';
	base = basename(path);
	ptr = strrchr(base, '.');
	if (!ptr)
		goto out;

	last = ptr;
	while (1) {
		ptr = strchr(ptr + 1, '.');
		if (!ptr)
			break;

		last = ptr;
	}

	last = strtolower(last);
	if (!strcmp(last, ".html") || !strcmp(last, ".htm"))
		return "text/html";

	if (!strcmp(last, ".css"))
		return "text/css";

	if (!strcmp(last, ".js"))
		return "application/javascript";

	if (!strcmp(last, ".png"))
		return "image/png";

	if (!strcmp(last, ".jpg") || !strcmp(last, ".jpeg"))
		return "image/jpeg";

	if (!strcmp(last, ".gif"))
		return "image/gif";

	if (!strcmp(last, ".ico"))
		return "image/x-icon";

out:
	return "application/octet-stream";
}

static int http_route_static(struct worker *wrk, struct client *cl)
{
	unsigned long long len;
	const char *mime;
	char path[2048];
	int ret;

	if (strstr(cl->header.uri, ".."))
		return http_route_404(wrk, cl);

	snprintf(path, sizeof(path), "web_data%s", cl->header.uri);
	ret = http_res_add_body_file(cl, path);
	if (ret < 0)
		return http_route_404(wrk, cl);

	mime = get_file_mime_by_ext(path);

	http_res_code(cl, 200);
	len = (unsigned long long)cl->res.body.st.st_size;
	ret |= http_res_add_hdr(cl, "Content-Type", mime);
	ret |= http_res_add_hdr(cl, "Content-Length", "%llu", len);
	if (ret)
		return -ENOMEM;

	return http_res_commit(wrk, cl);
}

static char *get_qs_val(const char *key, const char *qsq)
{
	char *ret = NULL;
	char *buf;
	char *pos;
	char *qs;
	int len;

	if (!qsq)
		return NULL;

	qs = strdup(qsq);
	if (!qs)
		return NULL;

	buf = malloc(strlen(key) + 2u);
	if (!buf)
		return NULL;

	len = snprintf(buf, strlen(key) + 2u, "%s=", key);
	pos = strstr(qs, buf);
	if (!pos)
		goto out;
	if (pos != qs && pos[-1] != '&')
		goto out;

	ret = pos + len;
	pos = strchr(ret, '&');
	if (pos)
		pos[0] = '\0';

	ret = strdup(ret);
	if (!ret)
		goto out;

	url_decode(ret, strlen(ret));
out:
	free(buf);
	free(qs);
	return ret;
}

static char *construct_query_get_user(struct mysql_conn *conn, const char *name,
			    	      int64_t limit, int64_t start_id)
{
	char *q;
	int ret;

	if (limit > 300)
		limit = 300;

	if (start_id < 1)
		start_id = 1;

	if (name) {
		char *tmp;
		size_t len;

		// Don't allow potential slow queries.
		while (*name == '%')
			name++;

		len = strlen(name);
		tmp = malloc(len * 2u + 1u);
		if (!tmp)
			return NULL;

		mysql_real_escape_string(conn->mysql, tmp, name, len);
		ret = asprintf(&q, "SELECT * FROM users WHERE _id >= %lld AND Nama_Rekening LIKE '%s%%' LIMIT %lld",
		               (long long)start_id, tmp, (long long)limit);
		free(tmp);
	} else {
		ret = asprintf(&q, "SELECT * FROM users WHERE _id >= %lld LIMIT %lld",
		               (long long)start_id, (long long)limit);
	}

	if (ret < 0)
		return NULL;

	printf("q = %s\n", q);
	return q;
}

/*
 * Expected JSON result:
 *
 * {
 * 	"fields": ["_id", "name", "blah"],
 * 	"rows": [
 * 	   [1, "foo", "blah"],
 * 	   [2, "bar", "blah"],
 * 	   [3, "baz", "blah"]
 * 	]
 * }
 */
static char *json_result_query_get_user(MYSQL *mysql)
{
	unsigned int num_fields;
	json_object *obj;
	unsigned int i;
	MYSQL_RES *res;
	MYSQL_ROW row;
	char *ret;

	res = mysql_store_result(mysql);
	if (!res)
		return NULL;

	num_fields = mysql_num_fields(res);
	obj = json_object_new_object();
	if (!obj) {
		mysql_free_result(res);
		return NULL;
	}

	json_object_object_add(obj, "fields", json_object_new_array());
	for (i = 0; i < num_fields; i++) {
		MYSQL_FIELD *field = mysql_fetch_field_direct(res, i);
		json_object_array_add(json_object_object_get(obj, "fields"),
				      json_object_new_string(field->name));
	}

	json_object_object_add(obj, "rows", json_object_new_array());
	while ((row = mysql_fetch_row(res))) {
		json_object *row_obj = json_object_new_array();
		unsigned long *lengths = mysql_fetch_lengths(res);

		for (i = 0; i < num_fields; i++) {
			if (row[i])
				json_object_array_add(row_obj,
						      json_object_new_string_len(row[i],
										lengths[i]));
			else
				json_object_array_add(row_obj, NULL);
		}

		json_object_array_add(json_object_object_get(obj, "rows"),
				      row_obj);
	}

	mysql_free_result(res);
	ret = strdup(json_object_to_json_string(obj));
	json_object_put(obj);
	return ret;
}

static char *query_get_user(struct mysql_conn *conn, const char *name,
			    int64_t limit, int64_t start_id)
{
	char *q;
	int ret;

	q = construct_query_get_user(conn, name, limit, start_id);
	if (!q)
		return NULL;

	ret = mysql_query(conn->mysql, q);
	free(q);
	if (ret) {
		fprintf(stderr, "Warning: mysql_query() failed: %s\n",
			mysql_error(conn->mysql));
		return NULL;
	}

	return json_result_query_get_user(conn->mysql);
}

static void destroy_json_query_result(char *res)
{
	free(res);
}

struct get_user_arg {
	struct mysql_conn *conn;
	struct worker *wrk;
	struct client *cl;
	char *start_id;
	char *limit;
	char *name;
};

static void gwbsid_get_user(void *argx)
{
	struct get_user_arg *arg = argx;
	int64_t start_id = 0;
	int64_t limit = 100;
	size_t len;
	char *res;
	int ret;

	if (arg->start_id) {
		start_id = strtoll(arg->start_id, NULL, 10);
		if (start_id < 0)
			start_id = 0;
	}

	if (arg->limit) {
		limit = strtoll(arg->limit, NULL, 10);
		if (limit < 0)
			limit = 100;
	}

	arg->conn = get_mysql_conn(arg->wrk);
	if (IS_ERR(arg->conn)) {
		fprintf(stderr, "Warning: Failed to get MySQL connection (wrk: %u)\n",
			arg->wrk->id);
		return;
	}

	res = query_get_user(arg->conn, arg->name, limit, start_id);
	if (!res) {
		fprintf(stderr, "Warning: Failed to query_get_user()\n");
		return;
	}

	if (client_get_refcnt(arg->cl) == 1) {
		printf("Client %s is gone, not sending response\n",
			get_str_ss(&arg->cl->addr));
		goto out;
	}

	len = strlen(res);
	http_res_code(arg->cl, 200);
	ret = http_res_add_body(arg->cl, res);
	if (ret)
		goto out;

	ret |= http_res_add_hdr(arg->cl, "Content-Type", "application/json");
	ret |= http_res_add_hdr(arg->cl, "Content-Length", "%zu", len);
	if (ret)
		goto out;

	ret = http_res_commit(arg->wrk, arg->cl);
	if (ret == 0) {
		notify_worker(arg->wrk);
	} else {
		put_client_slot(arg->wrk, arg->cl);
		arg->cl = NULL;
	}

out:
	destroy_json_query_result(res);
}

static void gwbsid_free_get_user(void *arg)
{
	struct get_user_arg *a = arg;

	if (a->conn && !IS_ERR(a->conn))
		put_mysql_conn(a->wrk, a->conn);

	if (a->cl)
		put_client_slot(a->wrk, a->cl);

	free(a->start_id);
	free(a->limit);
	free(a->name);
	free(a);
}

static int http_route_gwbsid_api_v1(struct worker *wrk, struct client *cl)
{
	struct get_user_arg *arg;
	int ret;

	arg = malloc(sizeof(*arg));
	if (!arg)
		return -ENOMEM;

	arg->start_id = get_qs_val("start_id", cl->header.qs);
	arg->limit = get_qs_val("limit", cl->header.qs);
	arg->name = get_qs_val("name", cl->header.qs);
	arg->conn = NULL;
	arg->wrk = wrk;
	arg->cl = cl;

	client_add_refcnt(cl, 1);
	ret = schedule_work(wrk, gwbsid_get_user, arg, gwbsid_free_get_user);
	if (ret < 0) {
		free(arg);
		client_sub_refcnt(cl, 1);
		return http_route_404(wrk, cl);
	}

	return 0;
}

static int exec_http_router(struct worker *wrk, struct client *cl)
{
	if (!strcmp(cl->header.uri, "/"))
		return http_route_index(wrk, cl);

	if (!strncmp(cl->header.uri, "/assets/", 8))
		return http_route_static(wrk, cl);

	if (!strcmp(cl->header.uri, "/gwbsid/api/v1/get_user"))
		return http_route_gwbsid_api_v1(wrk, cl);

	return http_route_404(wrk, cl);
}

static int handle_client_pollin(struct worker *wrk, struct client *cl)
{
	int ret;

	ret = do_recv(cl);
	if (ret < 0)
		goto out_kill;

	ret = parse_http_headers(cl);
	if (ret < 0)
		goto out_kill;

	if (!cl->got_header)
		return 0;

	ret = exec_http_router(wrk, cl);
	if (ret < 0)
		goto out_kill;

	return 0;

out_kill:
	wrk->kill_current = true;
	return 0;
}

static int handle_client_pollout(struct worker *wrk, struct client *cl)
{
	int ret;

	ret = http_queue_response(wrk, cl);
	if (ret < 0)
		wrk->kill_current = true;

	return ret;
}

static int _handle_client_event(struct worker *wrk, struct epoll_event *ev)
{
	struct client *cl = ev->data.ptr;
	int ret;

	clock_gettime(CLOCK_BOOTTIME, &cl->last_active);

	wrk->kill_current = false;
	if (ev->events & EPOLLIN) {
		ret = handle_client_pollin(wrk, cl);
		if (ret < 0 || wrk->kill_current)
			goto out_close;
	}

	if (ev->events & EPOLLOUT) {
		ret = handle_client_pollout(wrk, cl);
		if (ret < 0 || wrk->kill_current)
			goto out_close;
	}

	return 0;

out_close:
	del_client_from_epoll(wrk, cl);
	put_client_slot(wrk, cl);
	return 0;
}

static int handle_client_event(struct worker *wrk, struct epoll_event *ev)
{
	if (ev->events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
		put_client_slot(wrk, ev->data.ptr);
		return 0;
	}

	_handle_client_event(wrk, ev);
	return 0;
}

static int handle_event(struct worker *wrk, struct epoll_event *ev)
{
	if (!ev->data.u64)
		return accept_tcp_socket(wrk);

	if (ev->data.u64 == 1)
		return consume_eventfd(wrk);

	return handle_client_event(wrk, ev);
}

static int handle_events(struct worker *wrk, int nr)
{
	struct epoll_event *ev = wrk->events;
	int i, ret;

	for (i = 0; i < nr; i++) {
		ret = handle_event(wrk, &ev[i]);
		if (ret < 0)
			return ret;
	}

	return 0;
}

static void *run_worker(void *arg)
{
	struct worker *wrk = arg;
	struct context *ctx = wrk->ctx;
	int ret;

	atomic_fetch_add(&ctx->online_workers, 1u);

	while (!ctx->stop) {
		ret = poll_events(wrk);
		if (ret < 0)
			break;

		ret = handle_events(wrk, ret);
		if (ret < 0)
			break;
	}

	atomic_fetch_sub(&ctx->online_workers, 1u);
	ctx->stop = true;

	return ERR_PTR(ret);
}

static void destroy_workers(struct context *ctx)
{
	struct worker *wrk;
	uint32_t i;

	if (!ctx->workers)
		return;

	for (i = 0; i < ctx->nr_workers; i++) {
		wrk = &ctx->workers[i];
		printf("Destroying TCP worker %u...\n", i);

		/*
		 * The first thread is the main thread.
		 * Don't join it.
		 */
		if (i > 0) {
			pthread_kill(wrk->thread, SIGTERM);
			pthread_join(wrk->thread, NULL);
		}

		if (wrk->epoll_fd >= 0)
			close(wrk->epoll_fd);
		if (wrk->event_fd >= 0)
			close(wrk->event_fd);

		destroy_client_slot(&wrk->client_slot);
	}

	free(ctx->workers);
	ctx->workers = NULL;
	assert(atomic_load(&ctx->online_workers) == 0);
}

static void destroy_io_workers(struct context *ctx)
{
	struct io_worker *wrk;
	uint32_t i;

	if (!ctx->io_workers)
		return;

	pthread_mutex_lock(&ctx->work_queue.lock);
	pthread_cond_broadcast(&ctx->work_queue.cond);
	pthread_mutex_unlock(&ctx->work_queue.lock);

	printf("Destroying %u IO worker(s)...\n", ctx->nr_io_workers);
	for (i = 0; i < ctx->nr_io_workers; i++) {
		wrk = &ctx->io_workers[i];
		pthread_kill(wrk->thread, SIGTERM);
		pthread_join(wrk->thread, NULL);
	}

	free(ctx->io_workers);
	ctx->io_workers = NULL;
}

static void destroy_context(struct context *ctx)
{
	ctx->stop = true;
	destroy_io_workers(ctx);
	destroy_workers(ctx);
	destroy_work_queue(ctx);
	destroy_mysql_pool(ctx);
	destroy_tcp_socket(ctx);
}

static int run_web_server(struct context *ctx)
{
	int ret;

	ret = parse_mysql_env(&ctx->mysql_cred);
	if (ret)
		return -ret;

	ret = parse_tcp_bind_cfg(&ctx->tcp_bind_cfg);
	if (ret)
		return -ret;

	ret = init_tcp_socket(ctx);
	if (ret)
		return ret;

	ret = init_mysql_pool(ctx);
	if (ret)
		goto out;

	ret = init_work_queue(ctx);
	if (ret)
		goto out;

	ret = init_io_workers(ctx);
	if (ret)
		goto out;

	ret = init_workers(ctx);
	if (ret)
		goto out;

	ret = PTR_ERR(run_worker(&ctx->workers[0]));
out:
	destroy_context(ctx);
	return ret;
}

int main(void)
{
	pid_t pids[NR_FORK_WORKERS];
	struct context ctx;
	size_t i;
	int ret;

	memset(&ctx, 0, sizeof(ctx));
	g_ctx = &ctx;

	ret = setup_sigaction();
	if (ret)
		return -ret;

	if (ARRAY_SIZE(pids) > 1) {
		for (i = 0; i < ARRAY_SIZE(pids); i++) {
			pids[i] = fork();
			if (pids[i] == 0)
				return run_web_server(&ctx);

			if (pids[i] < 0) {
				ret = errno;
				perror("fork() in main()");
				return ret;
			}
		}
	}

	ret = run_web_server(&ctx);
	for (i = 0; i < ARRAY_SIZE(pids); i++)
		waitpid(pids[i], NULL, 0);

	return 0;
}
