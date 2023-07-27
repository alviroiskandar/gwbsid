
const g_qs = new URLSearchParams(window.location.search);
const layer_loading = gid("layer_loading");
const layer_table = gid("layer_table");
const layer_menu = gid("layer_menu");
const g_table_state = {
	cur_table: null,
	cur_page: 1,
	limit: 20,
	tables: {
		users: {
			nr_rows: 24453797
		}
	}
};

function start_tbl_loading()
{
	gq_hide(layer_table);
	gq_show(layer_loading);
}

function end_tbl_loading()
{
	gq_hide(layer_loading);
	gq_show(layer_table);
}

function fill_table_users(status, data)
{
	if (status != 200) {
		alert("Error: HTTP code" + status);
	} else {
		fill_table("ptbl_users", data, g_table_state.users);
	}
	end_tbl_loading();
}

function open_table_users(st)
{
	gwbsid_get_user_table(fill_table_users, null, st.limit,
			      (st.cur_page - 1) * st.limit);
}

function open_table()
{
	let st = g_table_state;
	let stb = st.tables[st.cur_table];
	let max_page = Math.ceil(stb.nr_rows / st.limit);

	start_tbl_loading();
	switch (st.cur_table) {
		case "users":
			open_table_users(st);
			break;
		default:
			alert("Table " + table + " is not found.");
			window.location = "?";
			return;
	}

	let tbid = "ptbl_" + st.cur_table;
	let prev_btn = deq("#" + tbid + " .btn_cage .prev-btn");
	let next_btn = deq("#" + tbid + " .btn_cage .next-btn");

	deq("#" + tbid + " h1 .cur_page").innerHTML = st.cur_page + " of " + max_page;
	if (st.cur_page == 1)
		gq_hide(prev_btn);
	else
		gq_show(prev_btn);

	if (st.cur_page == max_page)
		gq_hide(next_btn);
	else
		gq_show(next_btn);
}

function propagate_url_table_state()
{
	let table = g_qs.get("table");
	let cur_page = 1;
	let limit = 30;	

	if (g_qs.has("page"))
		cur_page = parseInt(g_qs.get("page"));

	if (g_qs.has("limit"))
		limit = parseInt(g_qs.get("limit"));

	g_table_state.cur_table = table;
	g_table_state.cur_page = cur_page;
	g_table_state.limit = limit;

	if (cur_page < 1) {
		g_table_state.cur_page = 1;
		apply_url_state();
	}
}

function apply_url_state()
{
	let st = g_table_state;
	let dest = "?table=" + st.cur_table +
		   "&page=" + st.cur_page +
		   "&limit=" + st.limit;

	window.history.pushState(st.cur_table, "", dest);
}

function handle_next_page()
{
	let st = g_table_state;
	st.cur_page++;
	apply_url_state();
	open_table();
}

function handle_prev_page()
{
	let st = g_table_state;
	if (st.cur_page > 1)
		st.cur_page--;
	apply_url_state();
	open_table();
}

function handle_jump_page()
{
	let page = prompt("Jump to page:");
	if (page == null)
		return;

	page = parseInt(page);
	if (isNaN(page)) {
		window.alert("Invalid page number.");
		return;
	}

	g_table_state.cur_page = page;
	apply_url_state();
	open_table();
}

function __start()
{
	if (g_qs.has("table")) {
		propagate_url_table_state();
		open_table();
	} else {
		gq_show(layer_menu);
	}
}

__start();

addEventListener("popstate", function (event) {
	__start();
});
