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

int dbf_file::get_field_no(const char* fname) const {
	for(register i = 0; i < fields_no; i++) {
		if(::strcmpi(fields[i].name, fname) == 0)
			return i;
	}
	send_exception("get_field_no()", "invalid field %s", fname);
	return -1;
}

int dbf_file::get_field_no_def(const char* fname, int iNotFound) const {   // New by eMeL
	for(register i = 0; i < fields_no; i++) {
		if(::strcmpi(fields[i].name, fname) == 0)
			return i;
	}

	return iNotFound;
}

const char* dbf_file::get_string(const char* fname) const {
	::memset(field, 0, sizeof(field));
	register i = get_field_no(fname);
	::memcpy(field, buffer + fields[i].offset, fields[i].length);
	return field;
}

dbf_field dbf_file::get_field(int field_no) const {
	if(!(field_no >= 0 && field_no < fields_no))
		send_exception("get_field()", "invalid field %d", name, field_no);
	return dbf_field(buffer, fields[field_no]);
}

dbf_field dbf_file::get_field(const char* field_name) const {
	return dbf_field(buffer, fields[get_field_no(field_name)]);
}

dbf_field::dbf_field(char* buffer, const dbf_field_desc& desc) :
	size(desc.length), dec(desc.dec), offset(desc.offset),
	type(desc.type), ptr(buffer) {
}

dbf_field::dbf_field() : size(0), dec(0), type(0), offset(0), ptr(0) {}

dbf_field::operator DateTy() const {
	if(ptr)
		switch(type) {
		case 'D':
			initialize();
			return DateTy(field, DateTy::XBASE);
		case 'C':
			if(size == 4) {
				initialize();
				return DateTy(field, DateTy::PACKED);
			}
			else if(size >= 8) {
				initialize();
				char buffer[9];
				::strncpy(buffer, field, 8);
				buffer[8] = 0;            
				return DateTy(buffer);
			}
		}
	return DateTy(JulTy(0l));
}

dbf_field::operator double() const {
	if(ptr) {
		switch(type) {
		case 'N':
		  	initialize();
			return ::atof(field);
		case 'C':
			if(size == 8) {
			  	initialize();
				double value;
				::memcpy(&value, field, sizeof(double));
				return value;
			}
		}
	}
	return 0.0;
}

dbf_field::operator bool() const {
	if(ptr) {
	  	initialize();
		if('L' == type && 'T' == field[0]) return true;
	}
	return false;
}

dbf_field::operator int() const {
	if(ptr) {
		switch(type) {
		case 'N':
		  	initialize();
			return ::atoi(field);
		case 'C':
			if(size == 4) {
			  	initialize();
				return (int)int32_image(field);
			}
		}
	}
	return 0;
}

char* dbf_field::c_str(strip_type strip, converter_t converter) const {
	if(!ptr) return 0;
  	initialize();
	char *pointer = field;
	if(converter) converter(pointer, size);
	if(pointer) {
		switch(strip) {
			case left:
				stripL(pointer);
				break;
			case right:
				stripT(pointer);
				break;
			case both:
				stripLT(pointer);
				break;
			case none:
				break;
		}
		return pointer;
	}
	return 0;
}

