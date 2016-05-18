using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

// http://www.clicketyclick.dk/databases/xbase/format/ndx_example.html#NDX_EXAMPLE


namespace NDbfReaderEx
{
  public abstract class IndexFileBase : IIndexFile, IDisposable
  {
    #region variables, properties --------------------------------------------------------------------------
    
    protected Stream  stream;

    public    bool      disposed { get; private set; }

    [CLSCompliant(false)]
    protected DbfTable _dbfTable;
    [CLSCompliant(false)]
    protected DbfRow   _row = null;

    private bool _skipDeleted = false;

    public bool skipDeleted 
    { 
      get { return GetSkipDeleted(); } 
      set { SetSkipDeleted(value);} 
    }

    //

    public bool GetSkipDeleted()    {return _skipDeleted;}

    public void SetSkipDeleted(bool newValue)
    {
      if (_row != null)
      {
        if (_row.deleted && newValue)
        {
          _row = null;                                                            // there isn't selected row  already
        }
      }

      _skipDeleted = newValue;
    }

    //

    internal IndexPageCache indexPageCache = null;

    public int indexPageCacheSize 
    { 
      get 
      {
        if (indexPageCache != null)
        {
          return indexPageCache.pageCount;
        }

        return 0;                                                                 // There isn't page cache
      } 

      set 
      {
        if (value == 0)
        {
          indexPageCache = null;                                                  // There isn't page cache                                           
        }
        else
        {
          indexPageCache.pageCount = value;
        }
      } 
    }

    //

    const int enabledKeyCharsLen = 128;                                           // valid byte codes in key is only 'low ASCII'

    public static bool[] enabledKeyChars = new bool[enabledKeyCharsLen];          // It can be modified by users if only it's a problem!
      
    #endregion

    #region constructor -----------------------------------------------------------------------------------

    internal IndexFileBase(Stream stream, DbfTable dbfTable, bool? skipDeleted = null, int indexPageCacheSize = 0)
    {
      disposed = false;

      if (stream == null)
      {
        throw ExceptionFactory.CreateArgumentException("stream", "IndexFileXXXX/stream is null!");
      }

     
      this.stream             = stream;
      this._dbfTable          = dbfTable;
      this.skipDeleted        = skipDeleted ?? dbfTable.skipDeleted;
      this.indexPageCacheSize = indexPageCacheSize;
    }


    static IndexFileBase()
    {
      for (byte i = (byte)'a'; (i <= (byte)'z'); i++)
      {
        enabledKeyChars[i] = true;
      }

      for (byte i = (byte)'A'; (i <= (byte)'Z'); i++)
      {
        enabledKeyChars[i] = true;
      }

      for (byte i = (byte)'0'; (i <= (byte)'9'); i++)
      {
        enabledKeyChars[i] = true;
      }

      enabledKeyChars[(byte)' '] = true;
      enabledKeyChars[(byte)'+'] = true;
      enabledKeyChars[(byte)'-'] = true;
      enabledKeyChars[(byte)'('] = true;
      enabledKeyChars[(byte)')'] = true;
    }
    #endregion

    #region interface -------------------------------------------------------------------------------------

    public int GetIndexPageCacheSize()         {return indexPageCacheSize;}

    [CLSCompliant(false)]
    public void   SetIndexPageCacheSize(int newValue)
    {
      indexPageCacheSize = newValue;
    } 
                     
    public void   ClearIndexPageCache()
    {
      if (indexPageCacheSize > 0)
      {                                                
        SetIndexPageCacheSize(0);                                               // set null the buffer (so clear all)
      }
    }                        
    #endregion

    #region IDisposable Members

    public void Dispose()
    {
      if (!disposed)
      {
        disposed = true;
        this.stream.Dispose();
        _row = null;
      }
    }

    ~IndexFileBase()
    { 
      Dispose();
    }
    #endregion

    #region IIndexFile Members

    public bool eof
    {
      get { return (_row == null); }
    }

    public DbfRow row
    {
      get { return _row; }
    }

    public DbfRow Next(int step = 1)
    {
      Debug.Assert((step > 0), "Parameter of Next() must greater than 0!");

      int recNo = 0;

      while (step > 0)
      {
        recNo = GetNext();

        if (recNo < 1)
        {
          break;
        }

        step--;
      }

      return GetRow(recNo);                                              // it returns null if pageNo    
    }

    public DbfRow Prev(int step = 1)
    {
      throw new NotImplementedException();
    }

    public DbfRow Top()
    {
      if (_dbfTable.recCount < 1)
      {
        return null;                                                              // don't found
      }

      return GetRow(GetTop());
    }

    public DbfRow Bottom()
    {
      if (_dbfTable.recCount < 1)
      {
        return null;                                                              // don't found
      }

      return GetRow(GetBottom());
    }

    public DbfRow Seek(params object[] keys)
    {
      throw new NotImplementedException();
    }

    public DbfRow SoftSeek(params object[] keys)
    {
      throw new NotImplementedException();
    }

    private DbfRow GetRow(int dbfRecNo)
    {
      if (dbfRecNo < 1)
      { // '< 1' because recno rule as dbase/index rule
        _row = null;                                                              // don't found
      }
      else
      {
        _row = _dbfTable.GetRow(dbfRecNo - 1, false);
      }

      return _row;
    }

    public DbfRow  Seek(byte[] key, byte? appendByte = 0x20)
    {
      if (key.Length > keyBytesLen)
      {
        throw ExceptionFactory.CreateArgumentException("key", "Key byte array length more then '{0}'!", keyBytesLen);
      }
      else if (key.Length < keyBytesLen)
      {
        if (appendByte == null)
        {
          throw ExceptionFactory.CreateArgumentException("key", "Key byte array length less then '{0}'!", keyBytesLen);
        }
        else
        {
          int startIx = key.Length;

          Array.Resize(ref key, keyBytesLen);

          for (int i = startIx; i < keyBytesLen; i++)
          {
            key[i] = (byte)appendByte;
          }

          throw new NotImplementedException();
        }
      }

      return GetRow(SeekKey(key, false));
    }

    public DbfRow  SoftSeek(byte[] key)
    {
      if (key.Length > keyBytesLen)
      {
        throw ExceptionFactory.CreateArgumentException("key", "Key byte array length more then '{0}'!", keyBytesLen);
      }

      return GetRow(SeekKey(key, true));
    }

    public DbfRow  Seek(string key, char? appendChar = null)
    {
      byte? appendByte = null;

      if (appendChar != null)
      {
        string tempString = new String((char)appendChar, 1);                                // is there a best way?
        var    tempByte   = _dbfTable.parametersReadOnly.encoding.GetBytes(tempString);
        
        appendByte = tempByte[0];
      }

      return Seek(_dbfTable.parametersReadOnly.encoding.GetBytes(key), appendByte);
    }

    public DbfRow  SoftSeek(string key)
    {
      return SoftSeek(_dbfTable.parametersReadOnly.encoding.GetBytes(key));
    }

    public string KeyExpression
    {
      get { return GetKeyExpression(); }
    }

    public int keyBytesLen
    {
      get { return GetKeyBytesLen(); }
    }

    #endregion

    #region IIndexFile / abstract functions ----------------------------------------------------------------

    protected abstract int  GetTop();
    protected abstract int  GetBottom();
    protected abstract int  GetNext();
    protected abstract int  GetPrev();
    protected abstract int  SeekKey(byte[] bytes, bool softSeek = false);

    public    abstract bool   IsStreamValid(bool throwException);
    public    abstract string GetKeyExpression();           
    
    protected abstract int  GetKeyBytesLen();        

    #endregion

    #region technical -------------------------------------------------------------------------------------
    
    static protected string ProcessKeyExpressionBuffer(byte[] buffer)
    { // Helper function for GetKeyExpression()
      int keyLen = 0;

      for (int i = 0; i < buffer.Length; i++)
      {
        keyLen = i;

        if (buffer[i] == (byte)0)
        {
          break;
        }

        if (! enabledKeyChars[buffer[i]])
        {
          return null;                                                          // Null return value for signals key error!
        }
      }

      if (keyLen < 1)
      {
        return null;
      }

      return Encoding.ASCII.GetString(buffer, 0, keyLen).Trim(); 
    }

    #endregion
  }
}
