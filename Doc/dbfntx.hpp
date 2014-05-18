/*
 * DBF/NTX reader.
 *
 * Sources:
 * Rick Spence, "Clipper Programming Guide", Mikrotrend Books.
 * Erik Bachmann, http://www.e-bachmann.dk, xbase.txt
 *
 * Author:
 * Boris Botstein
 * botstein@yahoo.com
 * www.geocities.com/botstein/
 */

#ifndef __DBF_OBJ
#define __DBF_OBJ

#include "R:\dbfntxLib\Stl.hpp"

const NTX_PAGE_SIZE = 1024;
const NTX_MAX_KEY_LENGTH = 256;

const DBF_MAX_FIELD_LENGTH = 256;
const DBF_HEADER_SIZE = 32;
const DBF_FIELD_DESC_SIZE = 32;


typedef int (tNtxKeyCompare)(const char *sFind, const char *sKeyItem, int iLen);


#pragma pack(push, 1)
typedef struct {
	unsigned char signature;
	char date[3];
	long rec_no;
	short length;
	short rec_len;
	char reserved1[3];
	char net[13];
	char reserved2[4];
} dbf_header;
#pragma pack(pop)

#pragma pack(push, 1)
typedef struct {
	char name[11];
	char type;
	long addr;
	unsigned char length;
	unsigned char dec;
	short net1;
	unsigned char id_area;
	short net2;
	unsigned char flag;
	char reserved[8];
	int offset; // My own field.
} dbf_field_desc;
#pragma pack(pop)

class dbf_file;

typedef struct {
	char name[11];
	char type;    
	unsigned char size, dec;
} field_t;

class key_t {
public:
	key_t(const char* _value = 0, size_t _length = 0);

	char value[NTX_MAX_KEY_LENGTH];
	size_t length;
};

struct key_compare : binary_function<key_t, key_t, bool> {
	bool operator()(const key_t& x, const key_t& y) const;
};

typedef long recno_t;
typedef map< key_t, recno_t, key_compare > keys_t;

typedef void (*expression_t)(const dbf_file& file, key_t& result);

class ntx_file {
protected:
#pragma pack(push, 1)
	struct ntx_root {
		unsigned short signature;          // must be equal to 03.
		unsigned short version;            // index system version.
		long root;                         // first page offset.
		long unused;                       // first unused page offset.
		unsigned short item_size;          // distance between keys in page.
		unsigned short key_size;           // key size.
		unsigned short key_dec;            // for numeric keys.
		unsigned short max_item;           // max count of keys, holded in page.
		unsigned short half_page;          // min count of keys, holded in page.
		char key_expr[NTX_MAX_KEY_LENGTH]; // expression.
		unsigned short unique;             // unique flag.
	};
#pragma pack(pop)

#pragma pack(push, 1)
	union page_image {
		unsigned short entries;
		unsigned char bytes[NTX_PAGE_SIZE];
		unsigned short shorts[NTX_PAGE_SIZE / 2];
	};
#pragma pack(pop)

#pragma pack(push, 1)
	struct ntx_item {
		long page;
		long rec_no;
		char key[NTX_MAX_KEY_LENGTH];
	};
#pragma pack(pop)

	class ntx_page {
	private:
		char* page;
		long parent;
		short entries;
		unsigned short item_size;
		mutable unsigned short index;

		const ntx_file* file;

    tNtxKeyCompare * NtxKeyCompare;

	public:
		__fastcall ntx_page(const ntx_file& _file, long _parent);
		~ntx_page();

		ntx_item* __fastcall operator[](unsigned short i) const {
			if(entries > 0) {
				index = i;
				return (ntx_item*)(page + i * item_size);
			}
			return 0;
		}

		bool valid() { return entries > 0; }
		operator ntx_item*() const { return operator[](index); }
		ntx_item* __fastcall find(long offset) const;
		int __fastcall find(const char* key, int key_len) const;
		long get_parent() const { return parent; }
		short get_entries() const { return entries; }
		unsigned short get_index() const { return index; }
		bool on_last() const { return index == entries; }
	};
	typedef value_smart_ref< ntx_page > ntx_page_t;

	friend class dbf_file;
	friend class ntx_file::ntx_page;

	typedef map< long, ntx_page_t, less<long> > ntx_table;

	ntx_file(ntx_file&); // Performs link error on call.
	ntx_file& operator=(const ntx_file& rhs); // too.

	mutable FILE* ntx_handle;

	char* name;

	ntx_root root;
	ntx_table image;

	ntx_page* page;
	long offset;

	char key[NTX_MAX_KEY_LENGTH + 1];

	void __fastcall set_key(const char* value = 0);
	int __fastcall compare_with(const char* value);

protected:
	void release();

	void get_root();
	long get_first_page() const { return root.root; }

	void __fastcall get_disk_page(long _offset, long parent);
	void __fastcall get_page(long _offset, long parent);
	void __fastcall get_page(long _offset);

	long get_next_up();
	long get_next_down();
	long get_prev_up();
	long get_prev_down();

	void send_exception(const char* whence, const char* message = 0) const;

public:
	ntx_file(const char* filename, bool update);
	~ntx_file();

	void reset();

	long get_top();
	long get_bottom();
	long get_next();
	long get_prev();

	long __fastcall find(const char* k, unsigned short k_len, int& compare);

	const char* get_key() const
  {
		if (page && page->valid())
    {
      return ((ntx_item*)(*page))->key;
    }
		return NULL;
	}

	unsigned short get_key_size() const { return root.key_size; }
};

//--- dbf_field --------------------------------------------------------------

class dbf_field {
private:
	friend class dbf_file;

	mutable char field[DBF_MAX_FIELD_LENGTH];
	char* ptr;
	int size, dec, offset;
	char type;

	dbf_field(char* buffer, const dbf_field_desc& desc);
	dbf_field();

	void initialize() const {
		::memcpy(field, ptr + offset, size);
		field[size] = 0;
	}

public:
	enum strip_type { left, right, both, none };

	int  get_size() const { return size; }
  int  get_dec()  const { return dec;  }                      // New by eMeL
  char get_type() const { return type; }                      // New by eMeL

	char* c_str(strip_type strip = none, converter_t converter = 0) const;
	operator DateTy() const;
	operator int() const;
	operator double() const;
	operator bool() const;
};

//--- dbf_file ---------------------------------------------------------------

class dbf_file {
protected:
	enum lock_type { l_none = 0, l_exclusive = 1, l_file = 2 };

	dbf_header header;
	dbf_field_desc* fields;
	int fields_no;

	FILE* dbf_handle;

	char* buffer;

	long record;
  bool __fastcall get_offset(long rec_no);          

	ntx_file* index;
	char* name;
	mutable char field[DBF_MAX_FIELD_LENGTH + 1]; // for get_field.

	dbf_file(dbf_file&); // Performs link error on call.
	dbf_file& operator=(const dbf_file& rhs); // too.

	void __fastcall initialize(const char* filename, size_t bufsize);


	void send_exception(const char* whence) const;
	void send_exception(const char* whence, const char* fmt, ...) const;

	// Fields && methods for destructive operations:
	bool update, modified;
	lock_type locked;

	void write(const void* data, size_t size, const char* whence);
	void write_header();
	void write_total(long total);

	void record_status(bool remove); // recall || delete.

public:
  bool            hide_deleted;                     // korábban protected
  bool __fastcall get_record(long rec_no);          // korábban protected
                                                    // get_record(1) --> TOP, get_record(lastrec()) --> BOTTOM

  long            get_reclen(void) {return header.rec_len;};    // new by eMeL  'get_buffer' használatához a hossz

  //

	enum find_type { bad, success, before, after };

	dbf_file(const char* path, const char* dbf_name, const char* ntx_name = 0,
		bool _update = false, size_t bufsize = 0);
	dbf_file(const char* dbf_name, const field_t* sketch);
	virtual ~dbf_file();

	void reset() {
		record = 0;
		if(buffer) ::memset(buffer, 0, header.rec_len + 1);
	}

	void reset_all() {
		reset();
		if(index) index->reset();
	}

	bool deleted() const {
		if(buffer) return buffer[0] == '*';
		return false;
	}

	long current() const { return record; }
	const char* get_name() const { return name; }
	long lastrec() const;                                 // fileméretbõl számítva

	bool get_top();
	bool get_bottom();
	bool get_next();
	bool get_prev();

	bool __fastcall find(const char* k, unsigned short k_len);
	bool __fastcall find(const char* k, unsigned short k_len, find_type& state);

	const char*   get_buffer() const { return buffer; }
	const char*   get_key() const
                    {
		                  if(index) return index->get_key();
		                  return NULL;
                    }

	dbf_field     get_field(int field_no) const;
	dbf_field     get_field(const char* field_name) const;

  const char *  get_field_name(int field_no) const               // New by eMeL
                    {return ((field_no >= 0) && (field_no <= fields_no)) ? fields[field_no].name : (char *)NULL ;} 

  int           get_fields_no(void) const { return fields_no; }  // New by eMeL

	int           get_field_no(const char* fname) const;
  int           get_field_no_def(const char* fname, int iNotFound = -1) const;  // New by eMeL

  bool          isFieldExists(const char* fname)  {return (get_field_no_def(fname) >= 0); };   // New by eMeL

	const char*   get_string(const char* fname) const;

	bool flock();
	void unlock();

	// Destructive methods:

	void remove() { record_status(true); }
	void recall() { record_status(false); }

	void append();
	void insert(const char* fname, const DateTy& date);
	void insert(const char* fname, int number);
	void insert(const char* fname, double number);
	void insert(const char* fname, const char* str);
	void commit();
};

#endif // __DBF_OBJ

