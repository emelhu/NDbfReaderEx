/*
 * DBF/NTX reader.
 * Written by Boris Botstein.
 */

#if __BCPLUSPLUS__ >= 0x0530
	#include <vcl.h>
	#pragma hdrstop
#else
	#include <stdio.h>
	#pragma hdrstop
#endif    

#include <share.h>

#include "dbfntx.hpp"

void ntx_file::send_exception(const char* whence, const char* message) const {
	char buff[512];
	::strcpy(buff, name); ::strcat(buff, "\n");
	::strcat(buff, whence); ::strcat(buff, "\n");
	if(message) ::strcat(buff, message);
	else ::strcat(buff, ::strerror(errno));
	throw Exception(buff);
}

ntx_file::ntx_file(const char* filename, bool update) :
	page(0), offset(0l), name(0) {
	name = new_str(filename);
	ntx_handle = ::_fsopen(filename, update ? "r+b" : "rb", SH_DENYNONE);
	if(!ntx_handle) send_exception("ntx_file::ntx_file()");
	::setvbuf(ntx_handle, 0, _IOFBF, 1024);
	reset();
}

ntx_file::~ntx_file() {
	if(name) delete[] name;
	release();
	::fclose(ntx_handle);
}

void __fastcall ntx_file::set_key(const char* value) {
	::memset(key, 0, sizeof(key));
	if(value) ::memcpy(key, value, get_key_size());
}

int __fastcall ntx_file::compare_with(const char* value) {
	return ::memcmp(key, value, get_key_size());
}

void ntx_file::get_root() {
	if(::fseek(ntx_handle, offset, SEEK_SET))
		send_exception("get_root()", "lseek error.");
	if(::fread(&root, sizeof(ntx_root), 1, ntx_handle) != 1)
		send_exception("get_root()", "can't read header.");
#if 0
	::printf("NTX ROOT:\n");
	::printf("signature == %d\n", root.signature);
	::printf("version == %d\n", root.version);
	::printf("root == %ld\n", root.root);
	::printf("unused == %ld\n", root.unused);
	::printf("item size == %d\n", root.item_size);
	::printf("key size == %d\n", root.key_size);
	::printf("key dec == %d\n", root.key_dec);
	::printf("max item == %d\n", root.max_item);
	::printf("half page == %d\n", root.half_page);
	::printf("expression == %s\n", root.key_expr);
	::printf("unique == %d\n\n", root.unique);
#endif
	if((root.key_size > NTX_MAX_KEY_LENGTH) ||
		(root.item_size != (unsigned short)(root.key_size + (sizeof(long) << 1))))
		send_exception("get_root()", "invalid root page.");
}

void __fastcall ntx_file::get_disk_page(long _offset, long parent) {
	if(_offset % 1024) send_exception("get_disk_page()", "invalid offset.");
	if(::ftell(ntx_handle) != _offset)
		if(::fseek(ntx_handle, _offset, SEEK_SET))
			send_exception("get_disk_page()", "lseek error.");
	page = new ntx_page(*this, parent);
	offset = _offset;
	image.insert(ntx_table::value_type(_offset, ntx_page_t(page)));
}

void ntx_file::release() {
	image.erase(image.begin(), image.end());
	page = 0;
	offset = 0l;
}

void ntx_file::reset() {
	release();
	get_root();
	get_disk_page(get_first_page(), 0l);
	if(page->valid()) set_key((*page)[0]->key);
	else set_key();
}

void __fastcall ntx_file::get_page(long _offset, long parent) {
	ntx_table::const_iterator i = image.find(_offset);
	if(i != image.end()) {
		offset = _offset;
		page = (*i).second.pointer();
	}
	else get_disk_page(_offset, parent);
}

void __fastcall ntx_file::get_page(long _offset) {
	ntx_table::const_iterator i = image.find(_offset);
	if(i != image.end()) {
		offset = _offset;
		page = (*i).second.pointer();
	}
	else send_exception("get_page()", "can't find page.");
}

long ntx_file::get_top() {
	long current = 0l, next = get_first_page(), rec_no = 0l;
	ntx_item* item;
	while(next) {
		get_page(next, current);
		if(!page->valid()) return 0l;
		item = (*page)[0];
		current = next;
		next = item->page;
		rec_no = item->rec_no;
	}
	if(compare_with(item->key) < 0)
		send_exception("get_top()", "invalid order.");
	set_key(item->key);
	return rec_no;
}

long ntx_file::get_bottom() {
	long current = 0l, next = get_first_page();
	unsigned short entries;
	ntx_item* item;
	if(!next) return 0l;
	while(next) {
		get_page(next, current);
		if(!page->valid()) return 0l;
		entries = page->get_entries();
		item = (*page)[entries];
		current = next;
		next = item->page;
	}
	entries--;
	item = (*page)[entries];
	if(compare_with(item->key) > 0)
		send_exception("get_bottom()", "invalid order.");
	set_key(item->key);
	return item->rec_no;
}

long ntx_file::get_next_up() {
	long rec_no = 0l;
	if(page) {
		if(!page->valid()) return 0l;
		long parent, current;
		ntx_item* item;
		do {
			current = offset;
			parent = page->get_parent();
			if(!parent) return 0l;
			get_page(parent);
			if(!page->valid()) return 0l;
			item = page->find(current);
		} while(page->get_index() == page->get_entries());
		rec_no = item->rec_no;
		if(compare_with(item->key) > 0)
			send_exception("get_next_up()", "invalid order.");
		set_key(item->key);
	}
	return rec_no;
}

long ntx_file::get_next_down() {
	long rec_no = 0l, next, current;
	ntx_item* item;
	if(page) {
		if(!page->valid()) return 0l;
		item = *page;
		current = offset;
		next = item->page;
		while(next) {
			get_page(next, current);
			if(!page->valid()) return 0l;
			item = (*page)[0];
			rec_no = item->rec_no;
			current = next;
			next = item->page;
		}
		if(compare_with(item->key) > 0)
			send_exception("get_next_down()", "invalid order.");
		set_key(item->key);
	}
	return rec_no;
}

long ntx_file::get_next() {
	if(page) {
		if(!page->valid()) return 0l;
		unsigned short item_no = page->get_index();
		if(item_no < page->get_entries()) item_no++;
		ntx_item* item = (*page)[item_no];
		if(item_no < page->get_entries()) {
			if(!item->page) {
				if(compare_with(item->key) > 0)
					send_exception("get_next()", "invalid order.");
				set_key(item->key);
				return item->rec_no;
			}
			else return get_next_down();
		}
		else if(item->page) return get_next_down();
		else return get_next_up();
	}
	return 0l;
}

long ntx_file::get_prev_up() {
	long rec_no = 0l, parent;
	unsigned short item_no;
	ntx_item* item;
	if(page) {
		if(!page->valid()) return 0l;
		item_no = page->get_index();
		while(!item_no) {
			parent = page->get_parent();
			if(!parent) return 0l;
			get_page(parent);
			if(!page->valid()) return 0l;
			item_no = page->get_index();
		}
		item_no--;
		item = (*page)[item_no];
		rec_no = item->rec_no;
		if(compare_with(item->key) < 0)
			send_exception("get_prev_up()", "invalid order.");
		set_key(item->key);
	}
	return rec_no;
}

long ntx_file::get_prev_down() {
	long rec_no = 0l, next, current;
	ntx_item* item;
	if(page) {
		if(!page->valid()) return 0l;
		item = *page;
		current = offset;
		next = item->page;
		while(next) {
			get_page(next, current);
			if(!page->valid()) return 0l;
			item = (*page)[page->get_entries()];
			current = next;
			next = item->page;
		}
		unsigned short item_no = page->get_entries();
		item_no--;
		item = (*page)[item_no];
		rec_no = item->rec_no;
		if(compare_with(item->key) < 0)
			send_exception("get_prev_down()", "invalid order.");
		set_key(item->key);
	}
	return rec_no;
}

long ntx_file::get_prev() {
	if(page) {
		if(!page->valid()) return 0l;
		unsigned short item_no = page->get_index();
		ntx_item* item = (*page)[item_no];
		if(item->page) return get_prev_down();
		else if(item_no) {
			item_no--;
			item = (*page)[item_no];
			if(compare_with(item->key) < 0)
				send_exception("get_prev()", "invalid order.");
			set_key(item->key);
			return item->rec_no;
		}
		else return get_prev_up();
	}
	return 0l;
}

long __fastcall ntx_file::find(const char* k, unsigned short k_len, int& compare) {
	long current = 0l, next = get_first_page(), rec_no = 0l;
	ntx_item* item;
	int cmp;
	if(k_len > get_key_size())
		k_len = get_key_size();
	bool now_ok = false;
	bool go_up = false; // May be int with ++ && -- ?
	set_key(); // Bug 30.06.97
	while(next) {
		if(go_up) get_page(next);
		else get_page(next, current);
		if(!page->valid()) return 0l;
		current = next;
		cmp = ::sign(page->find(k, k_len));
		item = *page;
		if(cmp == 0) {
			if(go_up && page->on_last()) { // Bug 22.07.1997
				next = page->get_parent();
				now_ok = true;
			}
			else if(!go_up) {
				next = item->page;
				now_ok = true;
			}
			else next = 0l;
		}
		else {
			if(!go_up) go_up = page->on_last() ? false : now_ok;
			next = go_up ? page->get_parent() : item->page;
		}
		rec_no = item->rec_no;
	}
	set_key(item->key);
	compare = cmp;
	return rec_no;
}

