using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

// http://www.clicketyclick.dk/databases/xbase/format/ndx_example.html#NDX_EXAMPLE


namespace NDbfReaderEx
{
  internal abstract class IndexFileBase : IIndexFile, IDisposable
  {
    #region variables
    
    protected Stream  stream;

    public    bool      disposed { get; private set; }

    private   DbfTable _dbfTable;
    private   DbfRow   _row = null;

    public bool      skipDeleted;    
 
    //

    const int enabledKeyCharsLen = 128;                                           // valid byte codes in key is only 'low ASCII'

    public static bool[] enabledKeyChars = new bool[enabledKeyCharsLen];          // It can be modified by users if only it's a problem!
      
    #endregion

    internal IndexFileBase(Stream stream, DbfTable dbfTable, bool skipDeleted)
    {
      disposed = false;

      if (stream == null)
      {
        throw ExceptionFactory.CreateArgumentException("stream", "IndexFileXXXX/stream is null!");
      }

     
      this.stream = stream;
      this._dbfTable   = dbfTable;
      this.skipDeleted = skipDeleted;

      Top();
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

    public bool Next(int step = 1)
    {
      throw new NotImplementedException();
    }

    public bool Prev(int step = 1)
    {
      throw new NotImplementedException();
    }

    public bool Top()
    {
      throw new NotImplementedException();
    }

    public bool Bottom()
    {
      throw new NotImplementedException();
    }

    public bool Seek(params object[] keys)
    {
      throw new NotImplementedException();
    }

    public bool SoftSeek(params object[] keys)
    {
      throw new NotImplementedException();
    }

    public string KeyExpression
    {
      get { throw new NotImplementedException(); }
    }

    #endregion

    #region IIndexFile / abstract functions ----------------------------------------------------------------

    protected abstract int  GetTop();
    protected abstract int  GetBottom();
    protected abstract int  GetNext();
    protected abstract int  GetPrev();
    protected abstract int  SeekKey(byte[] bytes, bool softSeek = false);

    public    abstract bool   IsStreamValid(bool throwException);
    public    abstract byte[] GetKeyExpression();                           

    #endregion

    static protected byte[] ProcessKeyExpressionBuffer(byte[] buffer)
    { // Helper function for GetKeyExpression()
      for (int i = 0; i < buffer.Length; i++)
      {
        if (buffer[i] == (byte)0)
        {
          Array.Resize(ref buffer, i);
          break;
        }

        if (! enabledKeyChars[buffer[i]])
        {
          return null;                                                          // Null return value for signals key error!
        }
      }

      if (buffer.Length < 1)
      {
        return null;
      }

      return buffer;
    }
  }
}
