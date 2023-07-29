
#ifndef WEB_SRC__CONTROLLERS__INDEX_H
#define WEB_SRC__CONTROLLERS__INDEX_H

#include "web.h"

int route_index(struct context *ctx, struct client *cl);
int route_init_index(struct context *ctx);
int route_destroy_index(struct context *ctx);

#endif
