
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include "web_src/controllers/index.h"

static const char *index_html;
static size_t index_html_len;

int route_index(struct context *ctx, struct client *cl)
{
	int ret;

	ret = http_set_res_body_map(cl, (char *)index_html, 0, index_html_len,
				    false);
	if (ret)
		return ret;

	http_set_res_code(cl, 200);
	ret |= http_add_res_hdr(cl, "Content-Type", "text/html; charset=utf-8");
	ret |= http_add_res_hdr(cl, "Content-Length", "%zu", index_html_len);
	if (ret)
		return ret;

	return http_res_commit(ctx, cl);
}

int route_init_index(struct context *ctx)
{
	struct stat st;
	int fd, err;
	char *map;

	fd = open("web_data/views/index.html", O_RDONLY);
	if (fd < 0) {
		fd = -errno;
		perror("open() in route_init_index()");
		return fd;
	}

	err = fstat(fd, &st);
	if (err) {
		err = -errno;
		perror("fstat() in route_init_index()");
		close(fd);
		return err;
	}

	index_html_len = st.st_size;
	map = mmap(NULL, index_html_len, PROT_READ, MAP_PRIVATE, fd, 0);
	if (map == MAP_FAILED) {
		err = -errno;
		perror("mmap() in route_init_index()");
		close(fd);
		return err;
	}

	index_html = map;
	close(fd);
	return 0;
}

void route_destroy_index(struct context *ctx)
{
	if (index_html)
		munmap((void *)index_html, index_html_len);
}
