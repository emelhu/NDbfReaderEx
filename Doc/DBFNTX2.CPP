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

#include "dbfntx.hpp"

__fastcall ntx_file::ntx_page::ntx_page(const ntx_file& _file, long _parent) :
	file(&_file), parent(_parent), index(0), entries(-1),
	item_size(_file.root.item_size), page(0) {
	page_image data;
	if(::fread(&data, NTX_PAGE_SIZE, 1, _file.ntx_handle) != 1)
		_file.send_exception("ntx_page::ntx_page", "can't read page.");
	if(data.entries > _file.root.max_item)
		_file.send_exception("ntx_page::ntx_page", "invalid entries value.");
	if(data.entries) {
		entries = data.entries;
		page = new char[item_size * (entries + 1)];
		unsigned border = NTX_PAGE_SIZE - item_size;
		unsigned from = 0;
		for(register unsigned short i = 0; i <= entries; i++, from += item_size) {
			unsigned idx = *(data.shorts + i + 1);
			if(idx > border)
				_file.send_exception("ntx_page::ntx_page", "invalid internal offset value.");
			::memcpy(page + from, data.bytes + idx, item_size);
		}
	}
}

ntx_file::ntx_page::~ntx_page() {
	if(page) delete[] page;
}

ntx_file::ntx_item* __fastcall ntx_file::ntx_page::find(long offset) const {
	if(entries == -1) return 0;
	ntx_item* item;
	for(register unsigned short i = 0; i <= get_entries(); i++) {
		item = operator[](i);
		if(item->page == offset) {
			index = i;
			return item;
		}
	}
	file->send_exception("ntx_page::find()", "invalid page.");
	return 0;
}

int __fastcall ntx_file::ntx_page::find(const char* key, int key_len) const {
	if(entries == -1)
  {
    return -1;
  }

  int l = 0, r = get_entries();
  const char* keyInPage;
  
	while(l < r) {
		register i = (l + r) >> 1;

    keyInPage = operator[]((unsigned short)i)->key;

		if(::memcmp(key, keyInPage, key_len) > 0)
			l = i + 1;
		else
			r = i;
	}
	if(get_entries() == l && !operator[]((unsigned short)l)->page)
		index = (unsigned short)(l - 1);
	else
		index = (unsigned short)l;
	return ::memcmp(key, operator[](index)->key, key_len);
}

