// SPDX-License-Identifier: GPL-2.0-only
/*
 * Copyright (C) 2023  Alviro Iskandar Setiawan <alviro.iskandar@gnuweeb.org>
 */

#include "web.h"
#include "web_src/route.h"

static struct context *g_ctx;

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

int __push_stack(struct stack *st, uint32_t val)
{
	uint32_t top;

	top = st->top;
	if (top >= st->size)
		return -EOVERFLOW;

	st->data[top] = val;
	st->top = top + 1;
	return 0;
}

int __pop_stack(struct stack *st, uint32_t *val)
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

int push_stack(struct stack *st, uint32_t val)
{
	int ret;

	pthread_mutex_lock(&st->lock);
	ret = __push_stack(st, val);
	pthread_mutex_unlock(&st->lock);
	return ret;
}

uint32_t count_stack(struct stack *st)
{
	uint32_t ret;

	pthread_mutex_lock(&st->lock);
	ret = st->top;
	pthread_mutex_unlock(&st->lock);
	return ret;
}

int pop_stack(struct stack *st, uint32_t *val)
{
	int ret;

	pthread_mutex_lock(&st->lock);
	ret = __pop_stack(st, val);
	pthread_mutex_unlock(&st->lock);
	return ret;
}

int init_stack(struct stack *st, uint32_t size)
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

void destroy_stack(struct stack *st)
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

	hdr->content_length = CONTENT_LENGTH_UNINITIALIZED;
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

	if (hdr->buf) {
		free(hdr->buf);
		hdr->buf = NULL;
		hdr->buf_len = 0;
	} else {
		assert(hdr->buf_len == 0);
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
			body->buf.len = 0;
		} else {
			assert(body->buf.len == 0);
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
	case T_RES_BODY_MAP:
		if (body->map.map) {
			if (body->map.need_unmap)
				munmap(body->map.map, body->map.size);
			body->map.map = NULL;
			body->map.off = 0;
			body->map.size = 0;
		} else {
			assert(body->map.off == 0);
			assert(body->map.size == 0);
		}
		break;
	default:
		assert(0);
	}

	body->type = T_RES_BODY_UINITIALIZED;
}

static int init_http_res_body(struct http_res_body *body)
{
	memset(body, 0, sizeof(*body));
	body->type = T_RES_BODY_UINITIALIZED;
	return 0;
}

static void __reset_client_soft(struct client *cl)
{
	if (cl->buf) {
		/*
		 * We don't free the buffer here because we may
		 * reuse it later.
		 */
		assert(cl->buf_len);
		cl->buf_pos = 0;
	} else {
		assert(cl->buf_pos == 0);
		assert(cl->buf_len == 0);
	}

	destroy_http_req_hdr(&cl->req_hdr);
	destroy_http_res_hdr(&cl->res_hdr);
	destroy_http_req_body(&cl->req_body);
	destroy_http_res_body(&cl->res_body);
	cl->total_body_len = 0;
	cl->pollout_set = false;
}

static void reset_client_keep_alive(struct client *cl)
{
	assert(cl->fd >= 0);
	assert(cl->state == T_CL_IDLE);
	__reset_client_soft(cl);
}

static void reset_client(struct client *cl)
{
	if (cl->fd >= 0) {
		// printf("Closing a client connection (fd=%d; idx=%u; addr=%s)\n",
		//        cl->fd, cl->id, get_str_ss(&cl->addr));
		close(cl->fd);
		cl->fd = -1;
	}


	if (cl->buf) {
		assert(cl->buf_len);
		free(cl->buf);
		cl->buf = NULL;
		cl->buf_len = 0;
		cl->buf_pos = 0;
	}
	__reset_client_soft(cl);
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
	if (ret) {
		free(clients);
		return ret;
	}

	while (i--) {
		cl = &clients[i];
		cl->id = i;
		cl->fd = -1;
		ret = __push_stack(&slot->stack, i);
		reset_client(cl);
		assert(!ret);
	}

	slot->clients = clients;
	return 0;
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
		free(works);
		return -ret;
	}

	ret = pthread_cond_init(&queue->cond, NULL);
	if (ret) {
		errno = ret;
		perror("pthread_cond_init() in init_work_queue_queue()");
		pthread_mutex_destroy(&queue->lock);
		free(works);
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

	ctx->iter++;
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
	cl->last_active = ctx->now;
	// printf("Accepted a new client connection (fd=%d; idx=%u; addr=%s)\n",
	//        fd, cl->id, get_str_ss(&cl->addr));
	return 0;
}

static int handle_new_conn(struct context *ctx)
{
	struct sockaddr_uin46 addr;
	socklen_t len = sizeof(addr);
	int tcp_fd = ctx->tcp_fd;
	bool got_client = false;
	uint32_t counter = 0;
	int ret;

again:
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

	ret = __handle_new_conn(ctx, ret, &addr);
	if (ret)
		return ret;

	if (++counter < 128)
		goto again;

	return 0;
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

static char *strtolower(char *str)
{
	char *ret = str;

	while (str[0]) {
		str[0] = tolower(str[0]);
		str++;
	}

	return ret;
}

static int parse_hdr_req_parse_method_uri_qs(struct http_req_hdr *hdr,
					     char **second_line)
{
	char *buf, *end, *ptr;

	assert(!hdr->off_method);
	assert(!hdr->off_uri);
	assert(!hdr->off_version);
	assert(!hdr->off_qs);

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
	hdr->off_uri = (uint32_t)(end - buf);
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
	hdr->off_version = (uint32_t)(end - buf);
	if (strncmp(end, "HTTP/", 5u))
		return -EINVAL;

	return 0;
}

static int parse_hdr_req_parse_fields(char *buf, struct http_req_hdr *hdr)
{
	struct http_hdr_field_off *tmp, *fields = NULL;
	uint16_t nr_fields = 0u;
	uint16_t nr_alloc = 16u;
	char *ptr, *end;
	int err;

	assert(!hdr->fields);
	assert(!hdr->nr_fields);
	assert(hdr->content_length == CONTENT_LENGTH_UNINITIALIZED);

	ptr = buf;
	if (!ptr[0])
		return 0;

	fields = malloc(nr_alloc * sizeof(*fields));
	if (!fields) {
		errno = ENOMEM;
		perror("malloc() in parse_hdr_req_parse_fields()");
		return -ENOMEM;
	}

	hdr->content_length = CONTENT_LENGTH_NOT_PRESENT;
	err = -EINVAL;
	while (ptr[0]) {
		char *key, *val;

		if (ptr[0] == '\r' && ptr[1] == '\n')
			break;

		nr_fields++;

		if (nr_fields > nr_alloc) {
			nr_alloc *= 2;
			tmp = realloc(fields, nr_alloc * sizeof(*fields));
			if (!tmp) {
				errno = ENOMEM;
				perror("realloc() in parse_hdr_req_parse_fields()");
				err = -ENOMEM;
				goto out_err;
			}
			fields = tmp;
		}

		tmp = &fields[nr_fields - 1];
		tmp->off_key = (uint16_t)(ptr - hdr->buf);

		end = strchr(ptr, ':');
		if (!end)
			goto out_err;

		key = strtolower(ptr);
		*end++ = '\0';
		tmp->off_val = (uint16_t)(end - hdr->buf + 1u);

		val = end;
		ptr = strchr(end, '\r');
		if (!ptr)
			goto out_err;

		*ptr = '\0';
		if (ptr[1] != '\n')
			goto out_err;

		if (!strcmp("content-length", key)) {
			char *eptr;
			int64_t cl;

			cl = (int64_t)strtoll(val, &eptr, 10);
			if (eptr[0] != '\0')
				cl = CONTENT_LENGTH_INVALID;

			hdr->content_length = cl;
		} else if (!strcmp("transfer-encoding", key) && !strcmp("chunked", val)) {
			hdr->content_length = CONTENT_LENGTH_CHUNKED;
		}

		ptr += 2;
	}

	hdr->fields = fields;
	hdr->nr_fields = nr_fields;
	return 0;

out_err:
	free(fields);
	return err;
}

void *memdup(const void *buf, size_t len)
{
	void *ret;

	ret = malloc(len);
	if (!ret)
		return NULL;

	return memcpy(ret, buf, len);
}

static int handle_client_recv_event(struct context *ctx, struct client *cl);

static void http_access_log(struct context *ctx, struct client *cl)
{
	// const char *method = http_hdr_get_method(cl);
	// const char *uri = http_hdr_get_uri(cl);
	// const char *qs = http_hdr_get_qs(cl);
	// time_t t = time(NULL);
	// char time_buf[32];

	// strftime(time_buf, sizeof(time_buf), "%d/%b/%Y:%H:%M:%S %z",
	// 	 localtime(&t));

	// printf("[%s] %s | %hd | %s %s%s%s\n",
	//        time_buf,
	//        get_str_ss(&cl->addr),
	//        cl->res_hdr.status,
	//        method, uri, qs ? "?" : "", qs ? qs : "");
}

static bool is_eligible_for_keep_alive(struct client *cl)
{
	struct http_req_hdr *hdr = &cl->req_hdr;
	char *val;

	val = http_req_hdr_get_field(hdr, "connection");
	if (val)
		return !strcmp(val, "keep-alive");

	val = &hdr->buf[hdr->off_version];
	return !strcmp(val, "HTTP/1.1");
}

static int parse_http_req_hdr(struct context *ctx, struct client *cl)
{
	struct http_req_hdr *hdr = &cl->req_hdr;
	char *buf = cl->buf;
	char *second_line;
	char *crlf;
	size_t len;
	int ret;

	cl->state = T_CL_RECV_REQ_HDR;
	crlf = strstr(buf, "\r\n\r\n");
	if (!crlf)
		return 0;

	len = (size_t)(crlf - buf) + 5u;
	hdr->buf = memdup(buf, len);
	if (!hdr->buf) {
		errno = ENOMEM;
		perror("memdup() in parse_http_req_hdr()");
		return -ENOMEM;
	}

	hdr->buf[len - 1u] = '\0';
	hdr->buf_len = len - 1u;
	ret = parse_hdr_req_parse_method_uri_qs(hdr, &second_line);
	if (ret < 0)
		return ret;

	ret = parse_hdr_req_parse_fields(second_line, hdr);
	if (ret < 0)
		return ret;

	cl->keep_alive = is_eligible_for_keep_alive(cl);
	cl->state = T_CL_RECV_REQ_BODY;

	/*
	 * Is the body already in the buffer?
	 *
	 * If so, handle it. Otherwise, wait for the next
	 * recv() call.
	 */
	if (cl->buf_pos > hdr->buf_len) {
		memmove(cl->buf, &cl->buf[hdr->buf_len], cl->buf_pos - hdr->buf_len);
		cl->buf_pos -= hdr->buf_len;
	} else {
		cl->buf_pos = 0;
	}

	return handle_client_recv_event(ctx, cl);
}

static int parse_http_req_body(struct context *ctx, struct client *cl)
{
	struct http_req_hdr *hdr = &cl->req_hdr;

	switch (hdr->content_length) {
	case CONTENT_LENGTH_NOT_PRESENT:
	case CONTENT_LENGTH_CHUNKED:
	case CONTENT_LENGTH_INVALID:
		/*
		 * No body.
		 */
		cl->state = T_CL_ROUTING;
		return handle_client_recv_event(ctx, cl);
	case CONTENT_LENGTH_UNINITIALIZED:
		/*
		 * Must not happen.
		 */
		abort();
		break;
	}

	/*
	 * Content-Length is present.
	 */

	cl->total_body_len += cl->buf_pos;
	if (cl->total_body_len > hdr->content_length)
		return -EINVAL;

	if (cl->total_body_len == hdr->content_length) {
		cl->state = T_CL_ROUTING;
		return handle_client_recv_event(ctx, cl);
	}

	return 0;
}

static int handle_client_recv_event(struct context *ctx, struct client *cl)
{
	int ret = 0;

	switch (cl->state) {
	case T_CL_IDLE:
	case T_CL_RECV_REQ_HDR:
		ret = parse_http_req_hdr(ctx, cl);
		break;
	case T_CL_RECV_REQ_BODY:
		ret = parse_http_req_body(ctx, cl);
		break;
	case T_CL_ROUTING:
		ret = exec_http_route(ctx, cl);
		http_access_log(ctx, cl);
		break;
	case T_CL_SEND_RES_HDR:
	case T_CL_SEND_RES_BODY:
		break;
	default:
		printf("Invalid ct->state = %hhu (fd=%d; idx=%u; addr=%s)\n",
		       cl->state, cl->fd, cl->id, get_str_ss(&cl->addr));
		abort();
		break;
	}

	return ret;
}

static int handle_client_recv(struct context *ctx, struct client *cl)
{
	int ret;

	ret = do_recv(cl);
	if (ret)
		return ret;

	ret = handle_client_recv_event(ctx, cl);
	if (ret == -EAGAIN)
		return 0;

	return ret;
}

char *http_req_hdr_get_field(struct http_req_hdr *hdr, const char *key)
{
	struct http_hdr_field_off *fields = hdr->fields;
	uint16_t nr_fields = hdr->nr_fields;
	uint16_t i;

	for (i = 0; i < nr_fields; i++) {
		struct http_hdr_field_off *field = &fields[i];
		char *ptr = &hdr->buf[field->off_key];

		if (!strcmp(ptr, key))
			return &hdr->buf[field->off_val];
	}

	return NULL;
}

static int http_res_hdr_add_field(struct http_res_hdr *hdr, char *key, char *val)
{
	struct http_hdr_field *tmp, *fields = hdr->fields;
	uint16_t nr_fields = hdr->nr_fields;

	nr_fields++;
	tmp = realloc(fields, nr_fields * sizeof(*fields));
	if (!tmp) {
		errno = ENOMEM;
		perror("realloc() in http_res_hdr_add_field()");
		return -ENOMEM;
	}

	fields = tmp;
	hdr->fields = fields;
	hdr->nr_fields = nr_fields;

	tmp = &fields[nr_fields - 1];
	tmp->key = key;
	tmp->val = val;
	return 0;
}

int http_add_res_hdr(struct client *cl, const char *key, const char *val, ...)
{
	char *kkey, *vval;
	va_list ap;
	int ret;

	kkey = strdup(key);
	if (!kkey) {
		errno = ENOMEM;
		perror("strdup() in http_add_res_hdr()");
		return -ENOMEM;
	}

	va_start(ap, val);
	ret = vasprintf(&vval, val, ap);
	va_end(ap);
	if (ret < 0) {
		errno = ENOMEM;
		perror("vasprintf() in http_add_res_hdr()");
		free(kkey);
		return -ENOMEM;
	}

	ret = http_res_hdr_add_field(&cl->res_hdr, kkey, vval);
	if (ret) {
		free(kkey);
		free(vval);
		return ret;
	}

	return 0;
}

int http_add_res_body(struct client *cl, const void *buf, size_t size)
{
	struct http_res_body *body = &cl->res_body;
	struct res_body_buf *bufp = &body->buf;
	void *tmp;

	assert(body->type == T_RES_BODY_UINITIALIZED ||
	       body->type == T_RES_BODY_BUFFER);

	if (body->type == T_RES_BODY_UINITIALIZED) {
		body->type = T_RES_BODY_BUFFER;
		bufp->buf = memdup(buf, size);
		bufp->len = size;
		bufp->off = 0u;
		if (!bufp->buf) {
			errno = ENOMEM;
			perror("memdup() in http_add_res_body()");
			return -ENOMEM;
		}
		return 0;
	}

	tmp = realloc(bufp->buf, bufp->len + size);
	if (!tmp) {
		errno = ENOMEM;
		perror("realloc() in http_add_res_body()");
		return -ENOMEM;
	}

	bufp->buf = tmp;
	memcpy(&bufp->buf[bufp->len], buf, size);
	bufp->len += size;
	return 0;
}

int http_set_res_body_map(struct client *cl, char *amap, uint64_t off,
			  uint64_t size, bool need_unmap)
{
	struct http_res_body *body = &cl->res_body;
	struct res_body_map *map = &body->map;

	assert(body->type == T_RES_BODY_UINITIALIZED);
	body->type = T_RES_BODY_MAP;

	map->map = (uint8_t *)amap;
	map->off = off;
	map->size = size;
	map->need_unmap = need_unmap;

	return 0;
}

static const char *http_code_to_str(int code)
{
	switch (code) {
	case 200:
		return "OK";
	case 201:
		return "Created";
	case 202:
		return "Accepted";
	case 203:
		return "Non-Authoritative Information";
	case 204:
		return "No Content";
	case 205:
		return "Reset Content";
	case 206:
		return "Partial Content";
	case 301:
		return "Moved Permanently";
	case 302:
		return "Found";
	case 303:
		return "See Other";
	case 304:
		return "Not Modified";
	case 307:
		return "Temporary Redirect";
	case 308:
		return "Permanent Redirect";
	case 400:
		return "Bad Request";
	case 401:
		return "Unauthorized";
	case 403:
		return "Forbidden";
	case 404:
		return "Not Found";
	case 500:
		return "Internal Server Error";
	case 501:
		return "Not Implemented";
	case 503:
		return "Service Unavailable";
	default:
		return "";
	}
}

static int construct_http_res_hdr(struct client *cl)
{
	struct http_req_hdr *req_hdr = &cl->req_hdr;
	struct http_res_hdr *res_hdr = &cl->res_hdr;
	size_t len, i;
	char *buf;

	len = (size_t)snprintf(NULL, 0,
			       "%s %d %s\r\n\r\n",
			       &req_hdr->buf[req_hdr->off_version],
			       res_hdr->status,
			       http_code_to_str(res_hdr->status));

	for (i = 0; i < res_hdr->nr_fields; i++) {
		struct http_hdr_field *field = &res_hdr->fields[i];

		len += strlen(field->key);
		len += strlen(field->val);
		len += sizeof(": ") - 1;
		len += sizeof("\r\n") - 1;
	}

	buf = malloc(len + 1u);
	if (!buf) {
		errno = ENOMEM;
		perror("malloc() in construct_http_res_hdr()");
		return -ENOMEM;
	}

	len = (size_t)snprintf(buf, len + 1u,
			       "%s %d %s\r\n",
			       &req_hdr->buf[req_hdr->off_version],
			       res_hdr->status,
			       http_code_to_str(res_hdr->status));

	for (i = 0; i < res_hdr->nr_fields; i++) {
		struct http_hdr_field *field = &res_hdr->fields[i];

		len += sprintf(&buf[len], "%s: %s\r\n", field->key, field->val);
	}

	buf[len++] = '\r';
	buf[len++] = '\n';
	buf[len] = '\0';

	res_hdr->buf = buf;
	res_hdr->buf_len = len;
	res_hdr->buf_off = 0u;
	return 0;
}

int http_res_commit(struct context *ctx, struct client *cl)
{
	int ret;

	ret = construct_http_res_hdr(cl);
	if (ret)
		return ret;

	cl->ep_event->events |= EPOLLOUT;
	cl->state = T_CL_SEND_RES_HDR;
	return 0;
}

static int do_send(struct client *cl, const void *buf, size_t len)
{
	ssize_t ret;

	ret = send(cl->fd, buf, len, MSG_DONTWAIT);
	if (ret < 0) {
		ret = -errno;
		if (ret == -EAGAIN)
			return 0;

		perror("send() in do_send()");
	}

	return (int)ret;
}

static int handle_client_send_event(struct context *ctx, struct client *cl);

static int handle_client_send_event_header(struct context *ctx, struct client *cl)
{
	struct http_res_hdr *hdr = &cl->res_hdr;
	size_t len;
	char *buf;
	int ret;

	buf = hdr->buf + hdr->buf_off;
	len = hdr->buf_len - hdr->buf_off;
	ret = do_send(cl, buf, len);
	if (ret < 0)
		return ret;

	hdr->buf_off += (uint32_t)ret;
	if (hdr->buf_off < hdr->buf_len)
		return -EAGAIN;

	if (cl->res_body.type != T_RES_BODY_UINITIALIZED) {
		cl->state = T_CL_SEND_RES_BODY;
		return handle_client_send_event(ctx, cl);
	}

	return 0;
}

static int handle_client_send_body_map(struct context *ctx, struct client *cl)
{
	struct http_res_body *body = &cl->res_body;
	uint8_t *buf;
	size_t len;
	int ret;

	buf = body->map.map + body->map.off;
	len = body->map.size - body->map.off;
	ret = do_send(cl, buf, len);
	if (ret < 0)
		return ret;

	body->map.off += (uint32_t)ret;
	if (body->map.off < body->map.size)
		return -EAGAIN;

	return 0;
}

static int handle_client_send_body_buffer(struct context *ctx, struct client *cl)
{
	struct http_res_body *body = &cl->res_body;
	struct res_body_buf *bufp = &body->buf;
	uint8_t *buf;
	size_t len;
	int ret;

	buf = bufp->buf + bufp->off;
	len = bufp->len - bufp->off;
	ret = do_send(cl, buf, len);
	if (ret < 0)
		return ret;

	bufp->off += (uint32_t)ret;
	if (bufp->off < bufp->len)
		return -EAGAIN;

	return 0;
}

static int handle_client_send_event_body(struct context *ctx, struct client *cl)
{
	struct http_res_body *body = &cl->res_body;
	int ret = 0;

	switch (body->type) {
	case T_RES_BODY_BUFFER:
		ret = handle_client_send_body_buffer(ctx, cl);
		break;
	case T_RES_BODY_FD:
		ret = 0;
		break;
	case T_RES_BODY_MAP:
		ret = handle_client_send_body_map(ctx, cl);
		break;
	case T_RES_BODY_UINITIALIZED:
		/*
		 * Must not happen.
		 */
		abort();
		break;
	}

	if (!ret) {
		if (cl->keep_alive) {
			if (cl->pollout_set) {
				union epoll_data data;

				data.ptr = cl;
				ret = epoll_mod(ctx, cl->fd, EPOLLIN, data);
				if (ret)
					return ret;

				cl->pollout_set = false;
			}

			cl->state = T_CL_IDLE;
			reset_client_keep_alive(cl);
		} else {
			return -ENETRESET;
		}
	}

	return ret;
}

static int handle_client_send_event(struct context *ctx, struct client *cl)
{
	int ret = 0;

	switch (cl->state) {
	case T_CL_SEND_RES_HDR:
		ret = handle_client_send_event_header(ctx, cl);
		break;
	case T_CL_SEND_RES_BODY:
		ret = handle_client_send_event_body(ctx, cl);
		break;
	default:
		break;
	}

	if (ret == -EAGAIN && !cl->pollout_set) {
		union epoll_data data;

		printf("setting epollout!\n");
		data.ptr = cl;
		ret = epoll_mod(ctx, cl->fd, EPOLLIN | EPOLLOUT, data);
		if (ret)
			return ret;

		cl->pollout_set = true;
		return 0;
	}

	if (ret == -EAGAIN)
		return 0;

	return ret;
}

static int handle_client_send(struct context *ctx, struct client *cl)
{
	return handle_client_send_event(ctx, cl);
}

static int handle_client_event(struct context *ctx, struct epoll_event *event)
{
	struct client *cl = event->data.ptr;
	int ret = 0;

	cl->last_active = ctx->now;
	cl->ep_event = event;

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

	cl->ep_event = NULL;
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
	int i, ret = 0;

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
	exec_destroy_http_route(ctx);
}

static int run_app(struct context *ctx)
{
	int ret;

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

	ret = exec_init_http_route(ctx);
	if (ret)
		goto out;

	ret = run_event_loop(ctx);

out:
	destroy_context(ctx);
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
