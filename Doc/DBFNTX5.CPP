/*
 * DBF/NTX reader (destructive methods).
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
#include <io.h>

#include "dbfntx.hpp"

dbf_file::dbf_file(const char* dbf_name, const field_t* sketch) :
	index(0), buffer(0), record(0l), name(0), dbf_handle(0), fields(0),
	update(true), locked(l_exclusive), modified(false), hide_deleted(true) {
	name = ::new_str(dbf_name);
	if(!sketch) send_exception("dbf_file()", "empty sketch.");
	::memset(&header, 0, sizeof(header));
	for(fields_no = 0; sketch[fields_no].name[0]; fields_no++)
		header.rec_len += sketch[fields_no].size;
	header.rec_len++; // for 'deleted' sign.
	fields = new dbf_field_desc[fields_no];
	::memset(fields, 0, sizeof(dbf_field_desc) * fields_no);
	for(int i = 0, offset = 1; i < fields_no; i++) {
		::strcpy(fields[i].name, sketch[i].name);
		::strupr(fields[i].name);
		fields[i].type = sketch[i].type;
		fields[i].length = sketch[i].size;
		fields[i].dec = sketch[i].dec;
		fields[i].offset = offset;
		offset += fields[i].length;
	}
	dbf_handle = ::_fsopen(name, "w+b", SH_DENYRW);
	if(!dbf_handle) send_exception("dbf_file()");
	write_header();
	buffer = new char[header.rec_len + 1];
	::memset(buffer, ' ', header.rec_len);
	buffer[header.rec_len] = 0;
}

void dbf_file::write(const void* data, size_t size, const char* whence) {
	if(::fwrite(data, size, 1, dbf_handle) != 1) send_exception(whence);
}

void dbf_file::write_header() {
	DateTy date;
	header.signature = 0x3;
	header.date[0] = (char)(date.Year() % 100);
	header.date[1] = (char)(date.Month());
	header.date[2] = (char)(date.Day());
	header.length  = (short)(32 + fields_no * 32 + 2);
	write(&header, DBF_HEADER_SIZE, "write_header()");
	for(register i = 0; i < fields_no; i++)
		write(fields + i, DBF_FIELD_DESC_SIZE, "write_header()");
	write("\xD\x0", 2, "write_header()");
	if(::fflush(dbf_handle)) send_exception("write_header()");
}

void dbf_file::write_total(long total) {
	if(::fseek(dbf_handle, 4l, SEEK_SET)) send_exception("write_total()");
	write(&total, sizeof(long), "write_total()");
	header.rec_no = total;
}

bool dbf_file::flock() {
	if(locked != l_none) return true;
	if(::lock(fileno(dbf_handle), 1000000001L, 1000000000L) == 0) {
		locked = l_file;
		return true;
	}
	return false;
}

void dbf_file::unlock() {
	if(locked == l_file) {
		if(::unlock(fileno(dbf_handle), 1000000001L, 1000000000L))
			send_exception("unlock()");
		locked = l_none;
	}
}

void dbf_file::record_status(bool remove) {
	if(locked != l_none) {
		if(update && buffer && current() && get_offset(current())) {
			buffer[0] = remove ? '*' : ' ';
			write(buffer, 1, "resord_status()");
			if(::fflush(dbf_handle)) send_exception("resord_status()");
		}
		else send_exception("resord_status()", "can't update record.");
	}
	else send_exception("resord_status()", "not locked.");
}

void dbf_file::commit() {
	if(locked != l_none) {
		if(modified) {
			if(update && buffer && record) {
				if(get_offset(current())) {
					bool added = (record == (lastrec() + 1)); // append.
					write(buffer, header.rec_len, "commit()");
					if(added) {
						write("\x1A", 1, "commit()");
						write_total(record);
					}
					if(::fflush(dbf_handle)) send_exception("commit()");
					// Here we must update indices.
					//
					modified = false;
					return;
				}
				else send_exception("commit()");
			}
			send_exception("commit()", "can't update record.");
		}
	}
	else send_exception("commit()", "not locked.");
}

void dbf_file::append() {
	::memset(buffer, ' ', header.rec_len);
	buffer[header.rec_len] = 0;
	record = lastrec() + 1;
	modified = true;
}

void dbf_file::insert(const char* fname, const DateTy& date) {
	if(update && buffer && current()) {
		register i = get_field_no(fname);
		if(fields[i].type != 'D')
			send_exception("insert()", "invalid field %s (date)", fname);
		::strncpy(buffer + fields[i].offset, date.c_str(DateTy::XBASE), 8);
		modified = true;
		return;
	}
	send_exception("insert()", "can't update record.");
}

void dbf_file::insert(const char* fname, int number) {
	if(update && buffer && current()) {
		register i = get_field_no(fname);
		char fmt[16], temp[256];
		::sprintf(fmt, "%%%dd", fields[i].length);
		::sprintf(temp, fmt, number);
		if(fields[i].type != 'N' || size_t(fields[i].length) != ::strlen(temp))
			send_exception("insert()", "invalid field %s (int)", fname);
		::strncpy(buffer + fields[i].offset, temp, fields[i].length);
		modified = true;
		return;
	}
	send_exception("insert()", "can't update record.");
}

void dbf_file::insert(const char* fname, double number) {
	if(update && buffer && current()) {
		register i = get_field_no(fname);
		char fmt[16], temp[256];
		::sprintf(fmt, "%%%d.%df", fields[i].length, fields[i].dec);
		::sprintf(temp, fmt, number);
		if(fields[i].type != 'N' || size_t(fields[i].length) != ::strlen(temp))
			send_exception("insert()", "invalid field %s (double)", fname);
		::strncpy(buffer + fields[i].offset, temp, fields[i].length);
		modified = true;
		return;
	}
	send_exception("insert()", "can't update record.");
}

void dbf_file::insert(const char* fname, const char* str) {
	if(update && buffer && current()) {
		register i = get_field_no(fname);
		int o = fields[i].offset;
		int l = fields[i].length;
		::strncpy(buffer + o, str, l);
		for(register i = 0; i < l; i++)
			if(*(buffer + o + i) == 0) *(buffer + o + i) = ' ';
		modified = true;
		return;
	}
	send_exception("insert()", "can't update record.");
}

// REINDEX --------------------------------------------------------------------

key_t::key_t(const char* _value, size_t _length) {
	length = (_length > NTX_MAX_KEY_LENGTH) ? NTX_MAX_KEY_LENGTH : _length;
	if(_value) ::memcpy(value, _value, length);
	else ::memset(value, 0, sizeof(value));
}

bool key_compare::operator()(const key_t& x, const key_t& y) const {
	if(x.length != y.length)
		throw Exception("key_compare()\nKeys MUST be with equals lengths.");
	return ::memcmp(&x, &y, x.length) < 0;
}

