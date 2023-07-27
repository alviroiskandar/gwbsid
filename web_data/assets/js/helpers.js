function gid(id)
{
	return document.getElementById(id);
}
function deq(el)
{
	return document.querySelector(el);
}
function ecr(el)
{
	return document.createElement(el);
}
function gwbsid_get_user_table(callback, name = null, limit = 100, start_id = 0)
{
	let xhr = new XMLHttpRequest();
	let url = "gwbsid/api/v1/get_user?limit=" + limit + "&start_id=" + start_id;

	if (name != null)
		url += "&name=" + encodeURIComponent(name);

	xhr.open("GET", url);
	xhr.onreadystatechange = function() {
		if (this.readyState == 4 && this.status == 200) {
			callback(this.status, JSON.parse(this.responseText));
		}
	};
	xhr.send();
}

function escape_html(unsafe)
{
	return unsafe
		.replace(/&/g, "&amp;")
		.replace(/</g, "&lt;")
		.replace(/>/g, "&gt;")
		.replace(/"/g, "&quot;")
		.replace(/'/g, "&#039;");
}

function fill_table(id, data, opt)
{
	let tb_head = deq("#" + id +" table thead");
	let tb_body = deq("#" + id +" table tbody");
	let i, it = 0;

	tb_head.innerHTML = tb_body.innerHTML = "";

	let tr = ecr("tr");
	for (i in data.fields) {
		let th = ecr("th");
		let val = data.fields[i];
		if (val === "_id")
			val = "seq_id";
		th.innerHTML = val;
		tr.appendChild(th);
	}
	tb_head.appendChild(tr);

	for (i in data.rows) {
		let j;

		it++;
		tr = ecr("tr");
		if (it % 2 == 0)
			tr.className = "tra";
		else
			tr.className = "trb";

		for (j in data.rows[i]) {
			let td = ecr("td");
			let val = data.rows[i][j];

			if (val === null)
				val = "<i>NULL</i>";
			else
				val = escape_html(val);

			td.innerHTML = val;
			tr.appendChild(td);
		}
		tb_body.appendChild(tr);
	}

	layer_table.style.display = "";
	layer_loading.style.display = "none";
}

function gq_hide(e)
{
	e.style.display = "none";
}

function gq_show(e)
{
	e.style.display = "";
}
