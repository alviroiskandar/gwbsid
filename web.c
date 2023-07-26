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

#define TCP_LISTEN_BACKLOG	1024
#define MYSQL_POOL_SIZE		128
#define NR_WORKERS		8
#define NR_WORK_ITEMS		1024
#define NR_EPOLL_EVENTS		32
#define NR_CLIENTS		1024
#define NR_FORK_WORKERS		4
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
	struct http_req_header	header;
	struct http_response	res;
	uint32_t		buf_len;
	uint32_t		buf_size;
	uint32_t		id;
	struct timespec		last_active;
	bool			pollout;
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

struct context {
	volatile bool		stop;
	int			tcp_fd;
	struct mysql_pool	mysql_pool;
	struct worker		*workers;
	struct work_queue	work_queue;
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
		return -EOVERFLOW;

	queue->items[tail & queue->mask] = *item;
	queue->tail = tail + 1;
	return 0;
}

static int __pop_work_queue(struct work_queue *queue, struct work_item *item)
{
	uint32_t head;

	head = queue->head;
	if (head == queue->tail)
		return -ENOENT;

	*item = queue->items[head & queue->mask];
	queue->head = head + 1;
	return 0;
}

static int push_work_queue(struct work_queue *queue, struct work_item *item)
{
	int ret;

	pthread_mutex_lock(&queue->lock);
	ret = __push_work_queue(queue, item);
	pthread_mutex_unlock(&queue->lock);
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

	if (queue->items) {
		free(queue->items);
		pthread_mutex_destroy(&queue->lock);
		pthread_cond_destroy(&queue->cond);
		memset(queue, 0, sizeof(*queue));
	}
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

static void put_client_slot(struct worker *wrk, struct client *client)
{
	int err;

	reset_client(client);
	err = push_stack(&wrk->client_slot.stack, client->id);
	if (err) {
		fprintf(stderr, "Warning: push_stack() failed in put_client_slot() (wrk=%u; id=%u)\n",
			wrk->id, client->id);
	}
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

	return ret;
}

static void set_worker_name(struct worker *wrk)
{
	char name[TASK_COMM_LEN];

	snprintf(name, sizeof(name), "ewrk-%u", wrk->id);
	pthread_setname_np(wrk->thread, name);
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

		set_worker_name(&wrk[i]);
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

	nr = epoll_wait(wrk->epoll_fd, ev, wrk->nr_events, -1);
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

static int install_client_to_worker(struct worker *wrk, struct worker *target_wrk,
				    struct client *cl)
{
	struct epoll_event ev;
	uint64_t val = 1;
	ssize_t wr_ret;
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

	wr_ret = write(target_wrk->event_fd, &val, sizeof(val));
	if (wr_ret < 0) {
		ret = -errno;
		perror("write() in install_client_to_worker()");
		return ret;
	}

	return 0;
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

static int parse_http_headers(struct client *cl)
{
	uint32_t len = cl->buf_len;
	char *buf = cl->buf;
	char *next;
	int ret;

	if (len < 4)
		return 0;

	if (!strstr(buf, "\r\n\r\n"))
		return 0;

	ret = parse_first_http_header(cl, &next);
	if (ret < 0)
		return ret;

	return parse_http_header_fields(cl, &next);
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

static int http_res_add_body_file(struct client *cl, const char *body)
{
	struct http_response *res = &cl->res;
	struct http_res_body *res_body = &res->body;
	int fd, ret;

	fd = open(body, O_RDONLY);
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

	len = (size_t)snprintf(NULL, 0, "HTTP/1.1 %d %s\r\n", res->header.code,
			       http_code_to_str(res->header.code));
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
	tmp = snprintf(buf, len, "HTTP/1.1 %d %s\r\n", res->header.code,
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

	if (ret == 0) {
		wrk->kill_current = true;
		return 0;
	}

	ret = do_send(cl, buf, (size_t)ret);
	if (ret < 0)
		return ret;

	cl->res.body.off += (uint32_t)ret;
	if (cl->res.body.off < (uint64_t)cl->res.body.st.st_size)
		return http_wrk_reg_pollout(wrk, cl);

	wrk->kill_current = true;
	return 0;
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

static int http_route_index(struct worker *wrk, struct client *cl)
{
	int ret;

	http_res_code(cl, 200);
	ret = http_res_add_body_file(cl, "web_data/views/index.html");
	if (ret < 0)
		return ret;

	ret |= http_res_add_hdr(cl, "Content-Type", "text/html");
	ret |= http_res_add_hdr(cl, "Content-Length", "%llu",
				(unsigned long long)cl->res.body.st.st_size);
	if (ret)
		return -ENOMEM;

	return http_res_commit(wrk, cl);
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

static int exec_http_router(struct worker *wrk, struct client *cl)
{
	if (!strcmp(cl->header.uri, "/"))
		return http_route_index(wrk, cl);

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
		if (ret < 0)
			return ret;
		if (wrk->kill_current)
			goto out_close;
	}

	if (ev->events & EPOLLOUT) {
		ret = handle_client_pollout(wrk, cl);
		if (ret < 0)
			return ret;
		if (wrk->kill_current)
			goto out_close;
	}

	return 0;

out_close:
	epoll_ctl(wrk->epoll_fd, EPOLL_CTL_DEL, cl->fd, NULL);
	put_client_slot(wrk, cl);
	return 0;
}

static int handle_client_event(struct worker *wrk, struct epoll_event *ev)
{
	if (ev->events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) {
		put_client_slot(wrk, ev->data.ptr);
		return 0;
	}

	return _handle_client_event(wrk, ev);
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

	pthread_mutex_lock(&ctx->work_queue.lock);
	pthread_cond_broadcast(&ctx->work_queue.cond);
	pthread_mutex_unlock(&ctx->work_queue.lock);

	for (i = 0; i < ctx->nr_workers; i++) {
		wrk = &ctx->workers[i];
		printf("Destroying worker %u...\n", i);

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

static void destroy_context(struct context *ctx)
{
	ctx->stop = true;
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
	pid_t pids[NR_FORK_WORKERS - 1];
	struct context ctx;
	size_t i;
	int ret;

	memset(&ctx, 0, sizeof(ctx));
	g_ctx = &ctx;

	ret = setup_sigaction();
	if (ret)
		return -ret;

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

	ret = run_web_server(&ctx);
	for (i = 0; i < ARRAY_SIZE(pids); i++)
		waitpid(pids[i], NULL, 0);

	return 0;
}
