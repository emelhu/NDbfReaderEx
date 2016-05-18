using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace NDbfReaderEx
{
  internal class IndexFileNDX : IndexFileBase
  {
    #region const -------------------------------------------------------------------------------------------

    public const int      pageSize  = 512;
    public const int      maxKeyLen = 256;        // ...!!!...
   
    #endregion

    #region variables ---------------------------------------------------------------------------------------

    NdxHeader header;

    #endregion

    public IndexFileNDX(Stream stream, DbfTable dbfTable, bool? skipDeleted = null, int indexPageCacheSize = 0)
      : base(stream, dbfTable, skipDeleted, indexPageCacheSize)               // 'stream', 'dbfTable' and 'skipDeleted' already stored by base class constructor   
    { 
      this.header = GetHeader(stream);                                        // fill 'header' & IsStreamValid/Exception if error

      //

      #if DEBUG
      List<string> list = new List<string>();

      for (int i = 1; (i < this.header.totalPages); i++)
      {
        list.Add(String.Empty);
        list.Add(String.Format("*** Page {0} *** {1}", i, (i == this.header.rootPage) ? "[ROOT]" : ""));

        var keyPages = KeyPageRead(i);

        foreach (var kp in keyPages)
        {
          string line = String.Empty;

          if (kp.leftPage > 0)
          {
            line = String.Format(">>page {0}: '", kp.leftPage);
          }
          else if (kp.recNo > 0)
          {
            line = String.Format("{0,5} rec: '", kp.recNo);
          }

          line += _dbfTable.parametersReadOnly.encoding.GetString(kp.key) + "'";

          list.Add(line);
        }
      }

      File.WriteAllLines(@".\IndexPages.txt", list, _dbfTable.parametersReadOnly.encoding);
      #endif
    }

    #region Key position ------------------------------------------------------------------------------------
    
    protected override int GetTop()
    {
      int keyPageNo = this.header.rootPage;

      while (keyPageNo > 0)
      {
        var keyPages = KeyPageRead(keyPageNo);

        keyPageNo = -1;

        foreach (var kp in keyPages)
        {
          if (kp.leftPage > 0)
          {
            keyPageNo = kp.leftPage;
            break;
          }
          else if (kp.recNo > 0)
          {
            return kp.recNo;
          }
        }
      }

      throw new Exception("Index search error! [top]");
    }

    protected override int GetBottom()
    {
      int  lastID      = this.header.rootPage;
      bool lastIsRecno = false;                                         

      while (lastID > 0)
      {
        if (lastIsRecno)
        {
          return lastID;
        }
        else
        {
          var keyPages = KeyPageRead(lastID);

          if (keyPages.Length > 0)
          {
            var lastKeyInfo = keyPages[keyPages.Length - 1];

            if (lastKeyInfo.leftPage > 0)
            {
              lastID      = lastKeyInfo.leftPage;
              lastIsRecno = false;
            }
            else
            {
              lastID      = lastKeyInfo.recNo;
              lastIsRecno = true;
            }
          }
          else
          {
            lastID = -1;
          }
        }
      }

      throw new Exception("Index search error! [bottom]");
    }

    protected override int GetNext()
    {
      throw new NotImplementedException();
    }

    protected override int GetPrev()
    {
      throw new NotImplementedException();
    }

    protected override int SeekKey(byte[] bytes, bool softSeek = false)
    {
      throw new NotImplementedException();
    }
    
    #endregion

    #region NDX info ----------------------------------------------------------------------------------------
    
    public override string GetKeyExpression()
    {
      return ProcessKeyExpressionBuffer(this.header.keyExpr);
    }

    protected override int  GetKeyBytesLen()
    {
      return header.keyLen;
    }

    static public string GetKeyExpression(Stream stream)
    { // from full buffer, IndexFileBase class will process it.                                    
      var header = GetHeader(stream);

      return ProcessKeyExpressionBuffer(header.keyExpr);
    }

    public override bool IsStreamValid(bool throwException)
    {
      return IsStreamValid(stream, throwException);
    }
    
    static public bool IsStreamValid(Stream stream, bool throwException = false)
    {
      try
      {
        GetHeader(stream);

        return true;
      }
      catch 
      {
        if (throwException)
        {
          throw;
        }
      }

      return false;
    }

    public static NdxHeader GetHeader(Stream stream)
    {
      if (stream.Length < (pageSize * 2))
      {
        throw ExceptionFactory.CreateArgumentOutOfRangeException("stream", "NDX index stream length '{0}' < '{1}'!", stream.Length, (pageSize * 2));
      }

      if ((stream.Length % pageSize) != 0)
      {
        throw ExceptionFactory.CreateArgumentOutOfRangeException("stream", "NDX index stream length ({0}) isn't a multiple of '{1}'!", stream.Length, pageSize);
      }

      //

      NdxHeader header = new NdxHeader();

      stream.Position = 0;   
 
      BinaryReader reader = new BinaryReader(stream);                           // don't use 'using (BinaryReader reader...' because 'using' dispose 'stream' too!

      header.rootPage    = reader.ReadInt32();
      header.totalPages  = reader.ReadInt32();

      reader.ReadInt32();                                                       // reserved space

      header.keyLen      = reader.ReadUInt16();
      header.keyPages    = reader.ReadUInt16();
      header.keyType     = reader.ReadUInt16();
      header.keyRecSize  = reader.ReadInt32();                                  // Size of key record is a multiplum of 4. Record size is 4 (Pointer to next page) + 4 (record number i dbf) + key size ( as a multiplum of 4 ). i.e. if the key size is 10, the record size is 20 (4+4+12)

      {
        int extraSpace = header.keyLen % 4;

        if (extraSpace != 0)
        {
          extraSpace += (4 - extraSpace);
        }

        header.keyRecSize = 4 + 4 + header.keyLen + extraSpace;                 // rewrite readed value because it was a not valid value
      }

      reader.ReadByte();                                                        // reserved space

      header.unique      = reader.ReadBoolean();

      header.keyExpr     = reader.ReadBytes(maxKeyLen);

      if (ProcessKeyExpressionBuffer(header.keyExpr) == null)
      {
        throw ExceptionFactory.CreateNotSupportedException("Content of NDX key expression bytes is envalid!");
      }

      return header;
    }
    #endregion

    #region KeyPageRead ------------------------------------------------------------------------------------

    private NdxKeyItem[] KeyPageRead(int pageNo)
    {
      if ((pageNo < 1) || (pageNo > 0x7FFFFF))
      {
        throw ExceptionFactory.CreateArgumentOutOfRangeException("pageNo", "NDX index key page position invalid! [{0}]'!", pageNo);
      }

      int newPosition = pageNo * pageSize; 

      if ((newPosition + pageSize) > stream.Length)
      {
        throw ExceptionFactory.CreateArgumentOutOfRangeException("pageNo", "NDX index key page position invalid! (filesize) [{0}]'!", pageNo);
      }

      bool isRoot = (pageNo == header.rootPage);                                // if root we must correct this data structure with a 'close' item

      stream.Position = newPosition;   
 
      BinaryReader reader = new BinaryReader(stream);                           // don't use 'using (BinaryReader reader...' because 'using' dispose 'stream' too!

      int itemsCount = reader.ReadInt32(); 

      NdxKeyItem[] keyItems = new NdxKeyItem[itemsCount + (isRoot ? 1 : 0)];

      for (int i = 0; i < itemsCount; i++)
      {
        NdxKeyItem keyItem = new NdxKeyItem();

        keyItem.leftPage = reader.ReadInt32(); 
        keyItem.recNo    = reader.ReadInt32(); 
        keyItem.key      = reader.ReadBytes(header.keyLen);

        if ((header.keyLen % 4) != 0)
        {
          reader.ReadBytes(4 - (header.keyLen % 4));
        }

        keyItems[i] = keyItem;
      }


      if (isRoot)
      {
        NdxKeyItem keyItem = new NdxKeyItem();

        keyItem.leftPage = reader.ReadInt32(); 
        keyItem.recNo    = 0; 
        keyItem.key      = new byte[header.keyLen];

        for(int i = 0; i < keyItem.key.Length; i++)
        { // fill all bytes of array
          keyItem.key[i] = 0xFF;
        }

        keyItems[itemsCount] = keyItem;
      }

      return keyItems;
    }
    #endregion
  }

  //*******************************************************************************************************

  [CLSCompliant(false)]
  public class NdxHeader 
  {
    public Int32    rootPage;                                                 // Starting page no.
    public Int32    totalPages;                                               // Total no of pages.
    public UInt16   keyLen;                                                   // Key length. 
    public UInt16   keyPages;                                                 // No of keys per page.                  
    public UInt16   keyType;                                                  // Key type: 0 = char; 1 = Num
    public Int32    keyRecSize;                                               // Size of key record
    public bool     unique;                                                   // Unique index flag
    public byte[]   keyExpr;                                                  // The actual expression on which the index was built.  
  };

  //*******************************************************************************************************

  internal struct NdxKeyItem 
  {
    public Int32    leftPage;                                                 // left page for other NdxKeyItem-s                                               
    public Int32    recNo;                                                    // DBF record for this key                                               
    public byte[]   key;                                                      // key value - ASCII/BYTE comparision                                              
  };
}

