
#include "web_src/route.h"
#include "web_src/controllers/index.h"

static int http_route_404(struct context *ctx, struct client *cl)
{
	int ret;

	http_set_res_code(cl, 404);
	ret = http_add_res_body(cl, "404 Not Found!\n", 15);
	if (ret)
		return ret;

	ret |= http_add_res_hdr(cl, "Content-Type", "text/plain; charset=utf-8");
	ret |= http_add_res_hdr(cl, "Content-Length", "15");
	if (ret)
		return ret;

	return http_res_commit(ctx, cl);
}

int exec_http_route(struct context *ctx, struct client *cl)
{
	char *uri = http_hdr_get_uri(cl);

	if (!strcmp(uri, "/"))
		return route_index(ctx, cl);

	return http_route_404(ctx, cl);
}

int exec_init_http_route(struct context *ctx)
{
	int ret;

	ret = route_init_index(ctx);
	if (ret)
		return ret;

	return ret;
}

int exec_destroy_http_route(struct context *ctx)
{
	int ret;

	ret = route_destroy_index(ctx);
	if (ret)
		return ret;

	return 0;
}
