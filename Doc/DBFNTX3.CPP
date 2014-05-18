/*
 * DBF/NTX reader.
 * Written by Boris Botstein.
 */

#if __BCPLUSPLUS__ >= 0x0530
	#include <vcl.h>
	#pragma hdrstop
#else
	#include <windows.h>
	#include <stdarg.h>
	#include <stdio.h>
	#pragma hdrstop
#endif    

#include <sys\stat.h>
#include <share.h>
#include <dir.h>

#include "dbfntx.hpp"

void dbf_file::send_exception(const char* whence) const {
	send_exception(whence, ::strerror(errno));
}

void dbf_file::send_exception(const char* whence, const char* fmt, ...) const {
	char buff[1024];
	va_list argptr;
	::memset(buff, 0, sizeof(buff));
	::strcpy(buff, name); ::strcat(buff, "\n");
	::strcat(buff, whence); ::strcat(buff, "\n");
	va_start(argptr, fmt);
	::vsprintf(buff + ::strlen(buff), fmt, argptr);
	va_end(argptr);
	throw Exception(buff);
}

dbf_file::dbf_file(const char* path, const char* dbf_name,
	const char* ntx_name, bool _update, size_t bufsize) :
	index(0), buffer(0), record(0l), name(0), dbf_handle(0), fields(0),
	update(_update), locked(l_none), modified(false), hide_deleted(true) {
	char name_buffer[MAXPATH];
	::strcpy(name_buffer, path);
	::strcat(name_buffer, dbf_name);
	initialize(name_buffer, bufsize);
	if(ntx_name) {
		::strcpy(name_buffer, path);
		::strcat(name_buffer, ntx_name);
		index = new ntx_file(name_buffer, update);
	}
	get_top();
}

dbf_file::~dbf_file() {
	if(buffer) delete[] buffer;
	if(index) delete index;
	if(fields) delete[] fields;
	if(dbf_handle) {
		try {
			unlock();
			if(::fclose(dbf_handle) != 0) send_exception("fclose()");
		}
		catch(Exception& e) {
#if __BCPLUSPLUS__ >= 0x0530
			::MessageBox(0, e.Message.c_str(), "DbfNtx DTOR", MB_ICONERROR | MB_OK | MB_TASKMODAL);
#else
			::MessageBox(0, e.what(), "DbfNtx DTOR", MB_ICONERROR | MB_OK | MB_TASKMODAL);
#endif
		}
	}
	if(name) delete[] name;
}

void _fastcall dbf_file::initialize(const char* filename, size_t bufsize) {
	name = ::new_str(filename);
	dbf_handle = ::_fsopen(filename, update ? "r+b" : "rb", SH_DENYNONE);
	if(!dbf_handle) send_exception("initialize()");
	if(::fread(&header, DBF_HEADER_SIZE, 1, dbf_handle) != 1)
		send_exception("initialize()", "can't read header");
	fields_no = (header.length / DBF_FIELD_DESC_SIZE) - 1;
	fields = new dbf_field_desc[fields_no];
	for(int i = 0, offset = 1; i < fields_no; i++) {
		if(::fread(fields + i, DBF_FIELD_DESC_SIZE, 1, dbf_handle) != 1)
			send_exception("initialize()", "can't read field");
		fields[i].offset = offset;
		offset += fields[i].length;
#if 0
		::printf("name == %s\n", fields[i].name);
		::printf("type == %c\n", fields[i].type);
		::printf("length == %d\n", (int)fields[i].length);
		::printf("dec == %d\n", (int)fields[i].dec);
		::printf("offset == %d\n\n", fields[i].offset);
#endif

	}
	buffer = new char[header.rec_len + 1];
	if(bufsize) ::setvbuf(dbf_handle, 0, _IOFBF, bufsize);
}

long dbf_file::lastrec() const {
	struct stat statbuf;
	if(::fstat(fileno(dbf_handle), &statbuf))
		send_exception("lastrec()");
	long rv = (statbuf.st_size - header.length) / header.rec_len;
	return (rv < 0) ? 0 : rv;
}

bool __fastcall dbf_file::get_offset(long rec_no) {
	if(rec_no > 0) {
		long offset = header.length + (header.rec_len * (rec_no - 1));
		if((::ftell(dbf_handle) == offset) && (!update)) return true;
		if(!::fseek(dbf_handle, offset, SEEK_SET)) return true;
	}
	reset();
	return false;
}

bool _fastcall dbf_file::get_record(long rec_no) {
	if(!get_offset(rec_no)) return false;
	if(::fread(buffer, header.rec_len, 1, dbf_handle) != 1) {
		reset();
		return false;
	}
	record = rec_no;
	return true;
}

bool dbf_file::get_top() {
	bool result;
	if(index) result = get_record(index->get_top());
	else result = get_record(1);
	if(hide_deleted) {
		while(result && deleted()) result = get_next();
	}
	return result;
}

bool dbf_file::get_bottom() {
	bool result;
	if(!index) {
		result = get_record(header.rec_no + 1);
		if(!result) result = get_record(header.rec_no);
	}
	else result = get_record(index->get_bottom());
	if(hide_deleted) {
		while(result && deleted()) result = get_prev();
	}
	return result;
}

bool dbf_file::get_next() {
	bool result;
	if(hide_deleted) {
		do {
			if(index) result = get_record(index->get_next());
			else result = get_record(record + 1);
		} while(result && deleted());
	}
	else {
		if(index) result = get_record(index->get_next());
		else result = get_record(record + 1);
	}
	return result;
}

bool dbf_file::get_prev() {
	bool result;
	if(hide_deleted) {
		do {
			if(index) result = get_record(index->get_prev());
			else result = get_record(record - 1);
		} while(result && deleted());
	}
	else {
		if(index) result = get_record(index->get_prev());
		else result = get_record(record - 1);
	}
	return result;
}

// index->find compares:
//  0: search == key
// -1: search < key
//  1: search > key
bool __fastcall dbf_file::find(const char* k, unsigned short k_len, find_type& state) {
	if(!k || !index) return false;
	int compare;
	if(k_len > index->get_key_size()) k_len = index->get_key_size();
	bool result = get_record(index->find(k, k_len, compare));
	if(hide_deleted) {
		while(result && deleted()) result = get_next();
	}
	if(result) {
		int rv = ::memcmp(k, index->get_key(), k_len);
		if(rv < 0) state = after; // k < index->get_key()
		else if(rv > 0) state = before; // k > index->get_key()
		else state = success;
	}
	else state = bad;
	return success == state;
}

bool __fastcall dbf_file::find(const char* k, unsigned short k_len) {
	find_type state;
	if(!find(k, k_len, state)) return false;
	return success == state;
}

