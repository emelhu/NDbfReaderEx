using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace NDbfReaderEx
{
  [CLSCompliant(false)]
  public interface IIndexFile
  {
    bool    eof   { get; }
    DbfRow  row   { get; }

    DbfRow  Next(int step = 1);
    DbfRow  Prev(int step = 1);
    
    DbfRow  Top();
    DbfRow  Bottom();

    DbfRow  Seek(params Object[] keys);
    DbfRow  SoftSeek(params Object[] keys);

    /// <summary>
    /// Seek a key value in index file and return founded DBF record.
    /// </summary>
    /// <param name="key"></param>
    /// <param name="appendByte">If parameter value is null size of 'key' parameter must identical as index key length.</param>
    /// <returns>A row of DBF file or null if key not found.</returns>
    DbfRow  Seek(byte[] key, byte? appendByte = 0x20);
    DbfRow  SoftSeek(byte[] key);

    DbfRow  Seek(string key, char? appendChar = ' ');
    DbfRow  SoftSeek(string key);

    //bool skipDeleted 
    //{ 
    //  get { return GetSkipDeleted(); } 
    //  set { SetSkipDeleted(value);} 
    //}

    bool GetSkipDeleted(); 
    void SetSkipDeleted(bool newValue);

    //UInt32 indexPageCacheSize 
    //{ 
    //  get { return GetIndexPageCacheSize(); } 
    //  set { SetIndexPageCacheSize(value);} 
    //}

    int     GetIndexPageCacheSize(); 
    void    SetIndexPageCacheSize(int newValue);                                // 0: don't use cache, other user can modify index

    void    ClearIndexPageCache();                                              // forget readed cache pages, read new content of disk index pages

    //

    string  KeyExpression {get;}

    bool    IsStreamValid(bool throwException = false);
  }
}
