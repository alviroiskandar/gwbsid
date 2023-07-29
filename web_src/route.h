
#ifndef WEB_SRC__ROUTE_H
#define WEB_SRC__ROUTE_H

#include "web.h"

int exec_http_route(struct context *ctx, struct client *cl);
int exec_init_http_route(struct context *ctx);
int exec_destroy_http_route(struct context *ctx);

#endif /* WEB_SRC__ROUTE_H */
