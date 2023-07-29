// SPDX-License-Identifier: GPL-2.0-only
/*
 * Copyright (C) 2023  Alviro Iskandar Setiawan <alviro.iskandar@gnuweeb.org>
 */

#ifndef WEB_H
#define WEB_H


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
#define NR_EPOLL_EVENTS		128
#define NR_CLIENTS		10240
#define NR_WQ_WORKERS		32
#define NR_WORKERS		1
#define ARRAY_SIZE(X)		(sizeof(X) / sizeof((X)[0]))

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
	T_RES_BODY_MAP          = 3,
};

enum {
	T_CL_IDLE          = 0,
	T_CL_RECV_REQ_HDR  = 1,
	T_CL_RECV_REQ_BODY = 2,
	T_CL_ROUTING       = 3,
	T_CL_SEND_RES_HDR  = 4,
	T_CL_SEND_RES_BODY = 5,
	T_CL_CLOSE         = 6,
};

struct http_hdr_field_off {
	uint16_t		off_key;
	uint16_t		off_val;
};

struct http_hdr_field {
	char			*key;
	char			*val;
};

enum {
	CONTENT_LENGTH_INVALID       = -4,
	CONTENT_LENGTH_UNINITIALIZED = -3,
	CONTENT_LENGTH_CHUNKED       = -2,
	CONTENT_LENGTH_NOT_PRESENT   = -1,
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
	int64_t				content_length;
};

struct http_req_body {
	char				*buf;
	uint16_t			buf_len;
};

struct http_res_hdr {
	struct http_hdr_field		*fields;
	char				*buf;
	uint16_t			nr_fields;
	uint16_t			buf_len;
	uint16_t			buf_off;
	int16_t				status;
};

struct res_body_fd {
	int		fd;
	uint64_t	off;
	uint64_t	size;
};

struct res_body_map {
	uint8_t		*map;
	uint64_t	off;
	uint64_t	size;
	bool		need_unmap;
};

struct res_body_buf {
	uint8_t	*buf;
	size_t	len;
	size_t	off;
};

struct http_res_body {
	uint8_t		type;
	union {
		struct res_body_buf	buf;
		struct res_body_fd	fd;
		struct res_body_map	map;
	};
};

struct client {
	int			fd;
	uint8_t			state;
	bool			pollout_set;
	bool			keep_alive;

	struct sockaddr_uin46	addr;

	char			*buf;

	struct epoll_event	*ep_event;

	/*
	 * The size of used buffer.
	 */
	uint32_t		buf_pos;

	/*
	 * The size of allocated buffer.
	 */
	uint32_t		buf_len;

	uint32_t		id;

	int64_t			total_body_len;

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
	uint32_t		iter;
};

void *memdup(const void *buf, size_t len);
void destroy_stack(struct stack *st);
int init_stack(struct stack *st, uint32_t size);
uint32_t count_stack(struct stack *st);
int pop_stack(struct stack *st, uint32_t *val);
int push_stack(struct stack *st, uint32_t val);
int __pop_stack(struct stack *st, uint32_t *val);
int __push_stack(struct stack *st, uint32_t val);

char *http_req_hdr_get_field(struct http_req_hdr *hdr, const char *key);
int http_add_res_hdr(struct client *cl, const char *key, const char *val, ...);
int http_set_res_body_map(struct client *cl, char *map, uint64_t off,
			  uint64_t size, bool need_unmap);

int http_add_res_body(struct client *cl, const void *buf, size_t size);

int http_res_commit(struct context *ctx, struct client *cl);

static inline void http_set_res_code(struct client *cl, int16_t code)
{
	cl->res_hdr.status = code;
}

static inline char *http_hdr_get_uri(struct client *cl)
{
	struct http_req_hdr *hdr = &cl->req_hdr;

	return &hdr->buf[hdr->off_uri];
}

static inline char *http_hdr_get_qs(struct client *cl)
{
	struct http_req_hdr *hdr = &cl->req_hdr;

	if (hdr->off_qs == (uint16_t)-1)
		return NULL;

	return &hdr->buf[hdr->off_qs];
}

static inline char *http_hdr_get_method(struct client *cl)
{
	struct http_req_hdr *hdr = &cl->req_hdr;

	return &hdr->buf[hdr->off_method];
}

static inline char *http_hdr_get_version(struct client *cl)
{
	struct http_req_hdr *hdr = &cl->req_hdr;

	return &hdr->buf[hdr->off_version];
}

#endif /* WEB_H */
