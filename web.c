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
#include <sys/mman.h>
#define CLIENT_BUF_SIZE		4096
#define TCP_LISTEN_BACKLOG	1024
#define MYSQL_POOL_SIZE		128
#define NR_WORK_ITEMS		1024
#define NR_EPOLL_EVENTS		32
#define NR_CLIENTS		1024
#define NR_WQ_WORKERS		32
#define NR_WORKERS		2
#define ARRAY_SIZE(X)		(sizeof(X) / sizeof((X)[0]))

enum {
	TASK_COMM_LEN = 16,
};

struct stack {
	uint32_t		*data;
	size_t			size;
	size_t			top;
	pthread_mutex_t		lock;
};

struct mysql_conn {
	MYSQL		*mysql;
	bool		is_used;
};

struct mysql_pool {
	struct mysql_conn	*conns;
	struct stack		stack;
};

struct sockaddr_uin46 {
	union {
		struct sockaddr		sa;
		struct sockaddr_in	in4;
		struct sockaddr_in6	in6;
	};
};

enum {
	T_RES_BODY_UINITIALIZED = 0,
	T_RES_BODY_BUFFER       = 1,
	T_RES_BODY_FD           = 2,
	T_RES_BODY_MAP_FD       = 3,
};

enum {
	T_CL_IDLE          = 0,
	T_CL_RECV_REQ_HDR  = 1,
	T_CL_RECV_REQ_BODY = 2,
	T_CL_SEND_RES_HDR  = 3,
	T_CL_SEND_RES_BODY = 4,
	T_CL_CLOSE         = 5,
};

struct http_hdr_field_off {
	uint16_t		off_key;
	uint16_t		off_val;
};

struct http_hdr_field {
	char			*key;
	char			*val;
};

struct http_req_hdr {
	char				*buf;
	struct http_hdr_field_off	*fields;
	uint16_t			buf_len;
	uint16_t			nr_fields;
	uint16_t			off_method;
	uint16_t			off_uri;
	uint16_t			off_qs;
	uint16_t			off_version;
};

struct http_req_body {
	char				*buf;
	uint16_t			buf_len;
};

struct http_res_hdr {
	struct http_hdr_field		*fields;
	uint16_t			nr_fields;
	int16_t				status;
};

struct res_body_fd {
	int		fd;
	uint64_t	off;
	uint64_t	size;
};

struct res_body_map_fd {
	uint8_t		*map;
	uint64_t	off;
	uint64_t	size;
};

struct res_body_buf {
	uint8_t		*buf;
	uint64_t	size;
};

struct http_res_body {
	uint8_t		type;
	union {
		struct res_body_buf	buf;
		struct res_body_fd	fd;
		struct res_body_map_fd	map_fd;
	};
};

struct client {
	int			fd;
	uint8_t			state;

	struct sockaddr_uin46	addr;

	char			*buf;

	/*
	 * The size of used buffer.
	 */
	uint32_t		buf_pos;

	/*
	 * The size of allocated buffer.
	 */
	uint32_t		buf_len;

	uint32_t		id;

	/*
	 * Last time the client is active. For timeout purposes.
	 */
	struct timespec		last_active;

	struct http_req_hdr	req_hdr;
	struct http_res_hdr	res_hdr;
	struct http_res_body	res_body;
	struct http_req_body	req_body;
};

struct client_slot {
	struct client	*clients;
	struct stack	stack;
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

struct work_queue;

struct wq_worker {
	bool			is_online;
	uint32_t		id;
	struct work_queue	*wq;
	pthread_t		thread;
};

struct context;

struct work_struct {
	void	(*callback)(struct work_queue *wq, void *arg);
	void	(*free)(void *arg);
	void	*arg;
};

struct wq_queue {
	struct work_struct	*works;
	pthread_mutex_t		lock;
	pthread_cond_t		cond;
	uint32_t		head;
	uint32_t		tail;
	uint32_t		mask;
};

struct work_queue {
	struct context		*ctx;
	struct wq_worker	*workers;
	struct wq_queue		queue;
	uint32_t		nr_workers;
};

struct context {
	/*
	 * Indicator whether the program should stop.
	 */
	volatile bool		stop;

	/*
	 * The epoll FD.
	 */
	int			epoll_fd;

	/*
	 * The main TCP socket that listens for incoming connections.
	 */
	int			tcp_fd;

	/*
	 * Event FD to wake up epoll_wait().
	 */
	int			event_fd;

	int			ep_timeout;

	uint32_t		nr_events;

	/*
	 * Client slot.
	 */
	struct client_slot	client_slots;

	struct timespec		now;

	struct epoll_event	*events;

	struct work_queue	wq;

	struct mysql_cred	mysql_cred;
	struct tcp_bind_cfg	tcp_bind_cfg;
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

static int fill_sockaddr_ss(struct sockaddr_storage *ss, const char *addr,
			    uint16_t port)
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

static const char *get_str_ss(struct sockaddr_uin46 *ss)
{
	static __thread char __buf[8][INET6_ADDRSTRLEN + 8];
	static __thread uint8_t __idx;

	char *buf, *ret;
	void *addr_ptr;
	uint16_t port;
	int family;

	family = ss->sa.sa_family;
	if (family != AF_INET6 && family != AF_INET)
		return "(unknown family)";

	ret = buf = __buf[__idx++ % 8];

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

	printf("Listening on %s...\n", get_str_ss((void *)&addr));
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
		ctx->tcp_fd = 0;
	}
}

static void destroy_http_req_hdr(struct http_req_hdr *hdr)
{
	if (hdr->buf) {
		free(hdr->buf);
		hdr->buf = NULL;
		hdr->buf_len = 0;
		hdr->off_method = 0;
		hdr->off_uri = 0;
		hdr->off_qs = 0;
		hdr->off_version = 0;
	} else {
		assert(hdr->buf_len == 0);
		assert(hdr->off_method == 0);
		assert(hdr->off_uri == 0);
		assert(hdr->off_qs == 0);
		assert(hdr->off_version == 0);
	}

	if (hdr->fields) {
		free(hdr->fields);
		hdr->fields = NULL;
		hdr->nr_fields = 0;
	} else {
		assert(hdr->nr_fields == 0);
	}
}

static int init_http_req_hdr(struct http_req_hdr *hdr)
{
	assert(!hdr->buf);
	assert(!hdr->buf_len);
	assert(!hdr->fields);
	assert(!hdr->nr_fields);
	return 0;
}

static void destroy_http_res_hdr(struct http_res_hdr *hdr)
{
	if (hdr->fields) {
		uint16_t i;

		for (i = 0; i < hdr->nr_fields; i++) {
			free(hdr->fields[i].key);
			free(hdr->fields[i].val);
		}

		free(hdr->fields);
		hdr->fields = NULL;
		hdr->nr_fields = 0;
	} else {
		assert(hdr->nr_fields == 0);
	}
}

static int init_http_res_hdr(struct http_res_hdr *hdr)
{
	assert(!hdr->fields);
	assert(!hdr->nr_fields);
	return 0;
}

static void destroy_http_req_body(struct http_req_body *body)
{
	if (body->buf) {
		free(body->buf);
		body->buf = NULL;
		body->buf_len = 0;
	} else {
		assert(body->buf_len == 0);
	}
}

static int init_http_req_body(struct http_req_body *body)
{
	body->buf_len = 4096u;
	body->buf = malloc(body->buf_len);
	if (!body->buf) {
		errno = ENOMEM;
		perror("malloc() in init_http_req_body()");
		return -ENOMEM;
	}

	body->buf[0] = '\0';
	return 0;
}

static void destroy_http_res_body(struct http_res_body *body)
{
	switch (body->type) {
	case T_RES_BODY_UINITIALIZED:
		break;
	case T_RES_BODY_BUFFER:
		if (body->buf.buf) {
			free(body->buf.buf);
			body->buf.buf = NULL;
			body->buf.size = 0;
		} else {
			assert(body->buf.size == 0);
		}
		break;
	case T_RES_BODY_FD:
		if (body->fd.fd >= 0) {
			close(body->fd.fd);
			body->fd.fd = 0;
			body->fd.off = 0;
			body->fd.size = 0;
		} else {
			assert(body->fd.off == 0);
			assert(body->fd.size == 0);
		}
		break;
	case T_RES_BODY_MAP_FD:
		if (body->map_fd.map) {
			munmap(body->map_fd.map, body->map_fd.size);
			body->map_fd.map = NULL;
			body->map_fd.off = 0;
			body->map_fd.size = 0;
		} else {
			assert(body->map_fd.off == 0);
			assert(body->map_fd.size == 0);
		}
		break;
	default:
		assert(0);
	}
}

static int init_http_res_body(struct http_res_body *body)
{
	body->type = T_RES_BODY_UINITIALIZED;
	body->buf.buf = NULL;
	body->buf.size = 0;
	return 0;
}

static void reset_client(struct client *cl)
{
	if (cl->fd >= 0) {
		printf("Closing a client connection (fd=%d; idx=%u, addr=%s)\n",
		       cl->fd, cl->id, get_str_ss(&cl->addr));
		close(cl->fd);
		cl->fd = -1;
	}

	if (cl->buf) {
		assert(cl->buf_len);
		free(cl->buf);
		cl->buf = NULL;
		cl->buf_len = 0;
		cl->buf_pos = 0;
	} else {
		assert(cl->buf_pos == 0);
		assert(cl->buf_len == 0);
	}

	destroy_http_req_hdr(&cl->req_hdr);
	destroy_http_res_hdr(&cl->res_hdr);
	destroy_http_req_body(&cl->req_body);
	destroy_http_res_body(&cl->res_body);
	memset(&cl->addr, 0, sizeof(cl->addr));
	memset(&cl->last_active, 0, sizeof(cl->last_active));
	cl->state = T_CL_CLOSE;
}

static int init_client_slot(struct context *ctx)
{
	struct client_slot *slot = &ctx->client_slots;
	struct client *clients, *cl;
	uint32_t i = NR_CLIENTS;
	int ret;

	clients = calloc(i, sizeof(*clients));
	if (!clients) {
		errno = ENOMEM;
		perror("calloc() in init_slot_slot()");
		return -ENOMEM;
	}

	ret = init_stack(&slot->stack, i);
	if (ret)
		goto out;

	while (i--) {
		cl = &clients[i];
		cl->id = i;
		cl->fd = -1;
		ret = push_stack(&slot->stack, i);
		reset_client(cl);
		assert(!ret);
	}

	slot->clients = clients;
	return 0;

out:
	return ret;
}

static int epoll_add(struct context *ctx, int fd, uint32_t events,
		     union epoll_data data)
{
	struct epoll_event ev = { .events = events, .data = data };
	int ret;

	ret = epoll_ctl(ctx->epoll_fd, EPOLL_CTL_ADD, fd, &ev);
	if (ret < 0) {
		ret = -errno;
		perror("epoll_ctl(EPOLL_CTL_ADD)");
		return ret;
	}

	return 0;
}

static int epoll_mod(struct context *ctx, int fd, uint32_t events,
		     union epoll_data data)
{
	struct epoll_event ev = { .events = events, .data = data };
	int ret;

	ret = epoll_ctl(ctx->epoll_fd, EPOLL_CTL_MOD, fd, &ev);
	if (ret < 0) {
		ret = -errno;
		perror("epoll_ctl(EPOLL_CTL_MOD)");
		return ret;
	}

	return 0;
}

static int epoll_del(struct context *ctx, int fd)
{
	int ret;

	ret = epoll_ctl(ctx->epoll_fd, EPOLL_CTL_DEL, fd, NULL);
	if (ret < 0) {
		ret = -errno;
		perror("epoll_ctl(EPOLL_CTL_DEL)");
		return ret;
	}

	return 0;
}

static int init_epoll(struct context *ctx)
{
	struct epoll_event *events;
	int tcp_fd = ctx->tcp_fd;
	int ret, epfd = -1, evfd = -1;
	union epoll_data data;

	ctx->ep_timeout = 3000;

	ctx->nr_events = NR_EPOLL_EVENTS;
	events = calloc(ctx->nr_events, sizeof(*events));
	if (!events) {
		errno = ENOMEM;
		perror("calloc() in init_epoll()");
		return -ENOMEM;
	}

	epfd = epoll_create(10000);
	if (epfd < 0) {
		ret = -errno;
		perror("epoll_create()");
		goto out_err;
	}

	evfd = eventfd(0, EFD_NONBLOCK);
	if (evfd < 0) {
		ret = -errno;
		perror("eventfd()");
		goto out_err;
	}

	ctx->events = events;
	ctx->epoll_fd = epfd;
	ctx->event_fd = evfd;

	data.u64 = 0;
	ret = epoll_add(ctx, tcp_fd, EPOLLIN, data);
	if (ret)
		goto out_err;

	data.u64 = 0;
	ret = epoll_add(ctx, evfd, EPOLLIN, data);
	if (ret)
		goto out_err;

	return 0;

out_err:
	free(events);
	ctx->events = NULL;
	if (epfd > 0) {
		close(epfd);
		ctx->epoll_fd = -1;
	}
	if (evfd > 0) {
		close(evfd);
		ctx->event_fd = -1;
	}
	return ret;
}

static int init_work_queue_queue(struct work_queue *wq)
{
	struct wq_queue *queue = &wq->queue;
	struct work_struct *works;
	uint32_t i = 1u;
	int ret;

	while (i < NR_WORK_ITEMS)
		i <<= 1;

	works = calloc(i, sizeof(*works));
	if (!works) {
		errno = ENOMEM;
		perror("calloc() in init_work_queue_queue()");
		return -ENOMEM;
	}

	ret = pthread_mutex_init(&queue->lock, NULL);
	if (ret) {
		errno = ret;
		perror("pthread_mutex_init() in init_work_queue_queue()");
		free(queue);
		return -ret;
	}

	ret = pthread_cond_init(&queue->cond, NULL);
	if (ret) {
		errno = ret;
		perror("pthread_cond_init() in init_work_queue_queue()");
		pthread_mutex_destroy(&queue->lock);
		free(queue);
		return -ret;
	}

	queue->works = works;
	queue->head = 0;
	queue->tail = 0;
	queue->mask = i - 1;
	return 0;
}

static void destroy_work_queue_queue(struct work_queue *wq)
{
	struct wq_queue *queue = &wq->queue;

	if (queue->works) {
		free(queue->works);
		pthread_mutex_destroy(&queue->lock);
		pthread_cond_destroy(&queue->cond);
		memset(queue, 0, sizeof(*queue));
	}
}

static void *run_wq_worker(void *arg)
{
	return NULL;
}

static void destroy_work_queue_workers(struct work_queue *wq)
{
	struct wq_worker *workers;
	uint32_t i;

	workers = wq->workers;
	if (!workers)
		return;

	i = wq->nr_workers;
	pthread_mutex_lock(&wq->queue.lock);
	pthread_cond_broadcast(&wq->queue.cond);
	pthread_mutex_unlock(&wq->queue.lock);

	while (i--) {
		struct wq_worker *wrk = &workers[i];

		pthread_kill(wrk->thread, SIGTERM);
		pthread_join(wrk->thread, NULL);
	}

	free(workers);
}

static int init_work_queue_workers(struct context *ctx)
{
	struct work_queue *wq = &ctx->wq;
	struct wq_worker *workers;
	uint32_t i, nr_workers;
	int ret;

	nr_workers = NR_WQ_WORKERS;
	workers = calloc(nr_workers, sizeof(*workers));
	if (!workers) {
		errno = ENOMEM;
		perror("calloc() in init_work_queue_workers()");
		return -ENOMEM;
	}

	wq->nr_workers = 0u;
	for (i = 0; i < nr_workers; i++) {
		struct wq_worker *wrk;

		wrk = &workers[i];
		wrk->id = i;
		wrk->wq = wq;
		ret = pthread_create(&wrk->thread, NULL, run_wq_worker, wrk);
		if (ret) {
			errno = ret;
			perror("pthread_create() in init_work_queue_workers()");
			goto out_err;
		}

		wq->nr_workers++;
	}

	wq->workers = workers;
	return 0;

out_err:
	destroy_work_queue_workers(wq);
	return -ret;
}

static int init_work_queue(struct context *ctx)
{
	int ret;

	ret = init_work_queue_queue(&ctx->wq);
	if (ret)
		return ret;

	ctx->wq.ctx = ctx;
	ret = init_work_queue_workers(ctx);
	if (ret) {
		destroy_work_queue_queue(&ctx->wq);
		return ret;
	}

	return 0;
}

static void destroy_work_queue(struct context *ctx)
{
	destroy_work_queue_workers(&ctx->wq);
	destroy_work_queue_queue(&ctx->wq);
}

static int poll_events(struct context *ctx)
{
	struct epoll_event *events = ctx->events;
	uint32_t nr_events = ctx->nr_events;
	int timeout = ctx->ep_timeout;
	int ep_fd = ctx->epoll_fd;
	int ret;
	
	ret = epoll_wait(ep_fd, events, nr_events, timeout);
	if (ret < 0) {
		ret = -errno;
		if (ret == -EINTR)
			return 0;

		perror("epoll_wait()");
		return ret;
	}

	if (ret > 0)
		clock_gettime(CLOCK_BOOTTIME, &ctx->now);

	return ret;
}

static int init_client(struct client *cl)
{
	int err;

	assert(cl->state == T_CL_CLOSE);
	cl->state = T_CL_IDLE;
	cl->buf_len = CLIENT_BUF_SIZE;
	cl->buf = malloc(cl->buf_len);
	if (!cl->buf) {
		err = -ENOMEM;
		perror("malloc() in init_client()");
		return err;
	}

	err = init_http_req_hdr(&cl->req_hdr);
	if (err)
		goto out_err;

	err = init_http_res_hdr(&cl->res_hdr);
	if (err)
		goto out_err;

	err = init_http_req_body(&cl->req_body);
	if (err)
		goto out_err;

	err = init_http_res_body(&cl->res_body);
	if (err)
		goto out_err;

	return 0;

out_err:
	reset_client(cl);
	return err;
}

static struct client *get_client_slot(struct context *ctx)
{
	struct client_slot *slot = &ctx->client_slots;
	struct client *ret;
	uint32_t id;
	int err;

	err = pop_stack(&slot->stack, &id);
	if (err)
		return ERR_PTR(err);

	ret = &slot->clients[id];
	err = init_client(ret);
	if (err) {
		push_stack(&slot->stack, id);
		return ERR_PTR(err);
	}

	assert(ret->id == id);
	return ret;
}

static void put_client_slot(struct context *ctx, struct client *cl)
{
	struct client_slot *slot = &ctx->client_slots;
	uint32_t id = cl->id;
	int err;

	if (cl->fd >= 0) {
		err = epoll_del(ctx, cl->fd);
		if (err)
			fprintf(stderr, "Failed to remove client from epoll: %d\n", err);
	}

	assert(cl->id == id);
	reset_client(cl);
	err = push_stack(&slot->stack, id);
	if (err) {
		fprintf(stderr, "Warning: Failed to put client slot back to stack: %d\n", err);
		return;
	}
}

static int do_accept(int tcp_fd, struct sockaddr_uin46 *addr, socklen_t *len,
		     bool *got_client)
{
	int fd;

	fd = accept4(tcp_fd, (struct sockaddr *)addr, len, SOCK_NONBLOCK);
	if (fd < 0) {
		*got_client = false;

		fd = -errno;
		if (fd == -EAGAIN)
			return 0;

		perror("accept4()");
		return fd;
	}

	*got_client = true;
	return fd;
}

static int __handle_new_conn(struct context *ctx, int fd,
			     struct sockaddr_uin46 *addr)

{
	union epoll_data ed;
	struct client *cl;
	int err;

	cl = get_client_slot(ctx);
	if (IS_ERR(cl)) {
		fprintf(stderr, "Failed to get client slot: %d\n", (int)PTR_ERR(cl));
		close(fd);
		return 0;
	}

	ed.ptr = cl;
	err = epoll_add(ctx, fd, EPOLLIN, ed);
	if (err) {
		fprintf(stderr, "Failed to add client to epoll: %d\n", err);
		put_client_slot(ctx, cl);
		close(fd);
		return 0;
	}

	cl->fd = fd;
	cl->addr = *addr;
	clock_gettime(CLOCK_BOOTTIME, &cl->last_active);
	printf("Accepted a new client connection (fd=%d; idx=%u, addr=%s)\n",
	       fd, cl->id, get_str_ss(&cl->addr));
	return 0;
}

static int handle_new_conn(struct context *ctx)
{
	struct sockaddr_uin46 addr;
	socklen_t len = sizeof(addr);
	int tcp_fd = ctx->tcp_fd;
	bool got_client = false;
	int ret;

	ret = do_accept(tcp_fd, &addr, &len, &got_client);
	if (ret < 0)
		return ret;

	if (!got_client)
		return 0;

	if (len > sizeof(addr)) {
		close(ret);
		fprintf(stderr, "Warning: accept4(): Truncated address (len = %u), dropping client...\n", len);
		return 0;
	}

	return __handle_new_conn(ctx, ret, &addr);
}

static int handle_event_fd(struct context *ctx)
{
	ssize_t rd_ret;
	uint64_t val;
	int ret;

	rd_ret = read(ctx->event_fd, &val, sizeof(val));
	if (rd_ret < 0) {
		ret = -errno;
		perror("read() in handle_event_fd()");
		return ret;
	}

	return 0;
}

static int do_recv(struct client *cl)
{
	ssize_t ret;
	size_t len;
	char *buf;

	buf = cl->buf + cl->buf_pos;
	len = cl->buf_len - 1u - cl->buf_pos;
	ret = recv(cl->fd, buf, len, MSG_DONTWAIT);
	if (ret < 0) {
		ret = -errno;
		if (ret == -EAGAIN)
			return 0;

		perror("recv()");
		return ret;
	}

	if (!ret)
		return -ECONNRESET;

	cl->buf_pos += (uint32_t)ret;
	cl->buf[cl->buf_pos] = '\0';
	return 0;
}

static char *http_hdr_get_uri(struct client *cl)
{
	struct http_req_hdr *hdr = &cl->req_hdr;

	return &hdr->buf[hdr->off_uri];
}

static char *http_hdr_get_qs(struct client *cl)
{
	struct http_req_hdr *hdr = &cl->req_hdr;

	if (hdr->off_qs == (uint16_t)-1)
		return NULL;

	return &hdr->buf[hdr->off_qs];
}

static char *http_hdr_get_method(struct client *cl)
{
	struct http_req_hdr *hdr = &cl->req_hdr;

	return &hdr->buf[hdr->off_method];
}

static char *http_hdr_get_version(struct client *cl)
{
	struct http_req_hdr *hdr = &cl->req_hdr;

	return &hdr->buf[hdr->off_version];
}

static int parse_hdr_req_parse_method_uri_qs(struct http_req_hdr *hdr,
					     char **second_line)
{
	char *buf, *end, *ptr;

	buf = hdr->buf;
	ptr = strchr(buf, '\r');
	ptr[0] = '\0';

	if (ptr[1] != '\n')
		return -EINVAL;

	*second_line = &ptr[2];

	/*
	 * Extract the method.
	 */
	hdr->off_method = 0u;
	end = strchr(buf, ' ');
	if (!end)
		return -EINVAL;

	*end++ = '\0';

	/*
	 * Extract the URI.
	 */
	hdr->off_uri = (uint32_t)(end - buf + 1u);
	if (end[0] != '/')
		return -EINVAL;

	end = strchr(end, ' ');
	if (!end)
		return -EINVAL;

	*end++ = '\0';


	/*
	 * Extract the query string.
	 */
	hdr->off_qs = -1;
	ptr = strchr(&buf[hdr->off_uri], '?');
	if (ptr) {
		hdr->off_qs = (uint32_t)(ptr - buf + 1u);
		*ptr++ = '\0';
	}

	/*
	 * Extract the HTTP version.
	 */
	hdr->off_version = (uint32_t)(end - buf + 1u);
	if (strncmp(end, "HTTP/", 5u))
		return -EINVAL;

	return 0;
}

static int parse_hdr_req_parse_fields(char *buf, struct http_req_hdr *hdr)
{
	struct http_hdr_field_off *tmp, *fields = NULL;
	uint16_t nr_fields = 0;
	char *ptr, *end;
	int err;

	ptr = buf;
	if (!ptr[0])
		return 0;

	err = -EINVAL;
	while (ptr[0]) {
		nr_fields++;
		tmp = realloc(fields, nr_fields * sizeof(*fields));
		if (!tmp) {
			errno = ENOMEM;
			perror("realloc() in parse_hdr_req_parse_fields()");
			err = -ENOMEM;
			goto out_err;
		}

		fields = tmp;
		tmp = &fields[nr_fields - 1];
		tmp->off_key = (uint16_t)(ptr - hdr->buf);

		end = strchr(ptr, ':');
		if (!end)
			goto out_err;

		*end++ = '\0';
		tmp->off_val = (uint16_t)(end - hdr->buf + 1u);

		ptr = strchr(end, '\r');
		if (!ptr)
			goto out_err;

		*ptr++ = '\0';
		if (ptr[0] != '\n')
			goto out_err;

		ptr++;
	}

	hdr->fields = fields;
	hdr->nr_fields = nr_fields;
	return 0;

out_err:
	free(fields);
	return err;
}

static int parse_http_req_hdr(struct client *cl)
{
	struct http_req_hdr *hdr = &cl->req_hdr;
	char *buf = cl->buf;
	char *second_line;
	char *crlf;
	int ret;

	cl->state = T_CL_RECV_REQ_HDR;
	crlf = strstr(buf, "\r\n\r\n");
	if (!crlf)
		return 0;

	hdr->buf = strndup(buf, crlf - buf);
	if (!hdr->buf) {
		errno = ENOMEM;
		perror("strndup() in parse_http_req_hdr()");
		return -ENOMEM;
	}

	ret = parse_hdr_req_parse_method_uri_qs(hdr, &second_line);
	if (ret < 0)
		return ret;

	/*
	 * Kill the LF, but don't kill the CR, the below function
	 * call needs the CR.
	 */
	crlf[1] = '\0';

	ret = parse_hdr_req_parse_fields(second_line, hdr);
	if (ret < 0)
		return ret;

	cl->state = T_CL_RECV_REQ_BODY;
	return 0;
}

static int handle_client_recv(struct context *ctx, struct client *cl)
{
	int ret;

	ret = do_recv(cl);
	if (ret)
		return ret;

	switch (cl->state) {
	case T_CL_IDLE:
		ret = parse_http_req_hdr(cl);
		break;
	}

	if (ret == -EAGAIN)
		return 0;

	return 0;
}

static int handle_client_send(struct context *ctx, struct client *cl)
{
	(void) ctx;
	(void) cl;
	return 0;
}

static int handle_client_event(struct context *ctx, struct epoll_event *event)
{
	struct client *cl = event->data.ptr;
	int ret = 0;

	cl->last_active = ctx->now;

	if (event->events & EPOLLIN) {
		ret = handle_client_recv(ctx, cl);
		if (ret)
			goto out;
	}

	if (event->events & EPOLLOUT) {
		ret = handle_client_send(ctx, cl);
		if (ret)
			goto out;
	}

	if (event->events & (EPOLLERR | EPOLLHUP))
		ret = 1;

out:
	if (ret)
		put_client_slot(ctx, cl);

	return 0;
}

static int handle_event(struct context *ctx, struct epoll_event *event)
{
	if (!event->data.u64)
		return handle_new_conn(ctx);

	if (event->data.u64 == 1)
		return handle_event_fd(ctx);

	return handle_client_event(ctx, event);
}

static int handle_events(struct context *ctx, int nr_polled_events)
{
	struct epoll_event *events = ctx->events;
	int i, ret;

	for (i = 0; i < nr_polled_events; i++) {
		ret = handle_event(ctx, &events[i]);
		if (ret < 0)
			break;
	}

	return ret;
}

static int run_event_loop(struct context *ctx)
{
	int ret = 0;

	while (!ctx->stop) {
		ret = poll_events(ctx);
		if (ret < 0)
			break;

		ret = handle_events(ctx, ret);
		if (ret < 0)
			break;
	}

	return ret;
}

static void destroy_epoll(struct context *ctx)
{
	if (ctx->epoll_fd) {
		close(ctx->epoll_fd);
		ctx->epoll_fd = 0;
	}

	if (ctx->event_fd) {
		close(ctx->event_fd);
		ctx->event_fd = 0;
	}

	if (ctx->events) {
		free(ctx->events);
		ctx->events = NULL;
	}
}

static void destroy_client_slot(struct context *ctx)
{
	struct client_slot *slot = &ctx->client_slots;
	uint32_t i;

	if (!slot->clients)
		return;

	for (i = 0; i < NR_CLIENTS; i++)
		reset_client(&slot->clients[i]);

	destroy_stack(&slot->stack);
	free(slot->clients);
}

static void destroy_context(struct context *ctx)
{
	destroy_work_queue(ctx);
	destroy_epoll(ctx);
	destroy_client_slot(ctx);
	destroy_tcp_socket(ctx);
}

static int run_app(struct context *ctx)
{
	int ret;

	ctx->epoll_fd = -1;
	ctx->tcp_fd = -1;

	ret = parse_mysql_env(&ctx->mysql_cred);
	if (ret)
		return ret;

	ret = parse_tcp_bind_cfg(&ctx->tcp_bind_cfg);
	if (ret)
		return ret;

	ret = init_tcp_socket(ctx);
	if (ret)
		return ret;

	ret = init_client_slot(ctx);
	if (ret)
		goto out;

	ret = init_epoll(ctx);
	if (ret)
		goto out;

	ret = init_work_queue(ctx);
	if (ret)
		goto out;

	ret = run_event_loop(ctx);
out:
	destroy_context(ctx);
	g_ctx = NULL;
	return ret;
}

static int spawn_workers(struct context *ctx)
{
	pid_t pids[NR_WORKERS];
	int ret, i;

	for (i = 1; i < NR_WORKERS; i++) {
		pids[i] = fork();
		if (!pids[i])
			return -run_app(ctx);

		if (pids[i] < 0) {
			ret = errno;
			perror("fork");
			goto out;
		}
	}

	ret = -run_app(ctx);

out:
	while (i--) {
		if (i > 0) {
			kill(pids[i], SIGTERM);
			waitpid(pids[i], NULL, 0);
		}
	}

	return -ret;
}

int main(void)
{
	struct context ctx;
	int ret;

	memset(&ctx, 0, sizeof(ctx));
	g_ctx = &ctx;

	ret = setup_sigaction();
	if (ret)
		return -ret;

	return spawn_workers(&ctx);
}
