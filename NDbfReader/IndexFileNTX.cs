using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

// http://www.clicketyclick.dk/databases/xbase/format/ntx.html
// http://www.clicketyclick.dk/databases/xbase/format/ntx.html#NTX_NOTE_12_SOURCE
// http://vivaclipper.wordpress.com/tag/sharing-data/
// http://www.ousob.com/ng/cldriv/ng24090.php
// http://linux.techass.com/projects/xdb/xbasedocs/xbase_c7.html
// http://www.wotsit.org/list.asp?al=N   //  http://www.wotsit.org/download.asp?f=ntx&sc=453462375

// http://www.phpkode.com/source/s/ntxfile/ntxfile/ntxclass.php
// dbfntx.hpp / dbfntx*.cpp by Boris Botstein // www.geocities.com/botstein/

// "supports the following data types for key expressions: Character / Numeric / Date / Logical"

// Open the NTX index read the header 1024 bytes long
// 1   (2)  : is the signature byte 06 signifying ntx index
// 3   (2)  : updates
// 5   (4)  : offset in the file of first index page
// 9   (4)  : offset to list of unused pages
// 13  (2)  : key size plus 8 bytes for pointers
// 15  (2)  : key size
// 17  (2)  : decimal places in key, if numeric
// 19  (2)  : maximum entries per page
// 21  (2)  : minimum number of entries per page
// 23  (256): key expression followed by a chr(0)
// 279 (1)  : 1 if unique, 0 if otherwise
// 280 (744): filter

namespace NDbfReaderEx
{
  internal class IndexFileNTX : IndexFileBase
  {
    #region const -------------------------------------------------------------------------------------------

    public const int      pageSize  = 1024;
    public const int      maxKeyLen = 256;

    public static byte[]  validSignatures = new byte[] {0x03, 0x06, 0x07, 0x26, 0x27};        // It can be modified by users if only it's a problem!

    #endregion

    #region variables ---------------------------------------------------------------------------------------

    NtxHeader header;

    #endregion

    public IndexFileNTX(Stream stream, DbfTable dbfTable, bool skipDeleted)
      : base(stream, dbfTable, skipDeleted)                                   // 'stream', 'dbfTable' and 'skipDeleted' already stored by base class constructor   
    { 
      IsStreamValid(stream, true);                                            // Exception if error

      FillHeader();                                                           // fill 'header'
    }

    #region Key position ------------------------------------------------------------------------------------
    
    protected override int GetTop()
    {
      throw new NotImplementedException();
    }

    protected override int GetBottom()
    {
      throw new NotImplementedException();
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

    #region NTX info ----------------------------------------------------------------------------------------
    
    public override byte[] GetKeyExpression()
    {
      return GetKeyExpression(stream);
    }

    public override bool IsStreamValid(bool throwException)
    {
      return IsStreamValid(stream, throwException);
    }
    
    static public bool IsStreamValid(Stream stream, bool throwException = false)
    {
      if (stream.Length < (pageSize * 2))
      {
        if (throwException)
        {
          throw ExceptionFactory.CreateArgumentOutOfRangeException("stream", "NTX index stream length '{0}' < '{1}'!", stream.Length, (pageSize * 2));
        }
        
        return false;
      }

      if ((stream.Length % pageSize) != 0)
      {
        if (throwException)
        {
          throw ExceptionFactory.CreateArgumentOutOfRangeException("stream", "NTX index stream length ({0}) isn't a multiple of '{1}'!", stream.Length, pageSize);
        }

        return false;
      }

      stream.Position = 0;

      BinaryReader reader = new BinaryReader(stream);                           // don't use 'using (BinaryReader reader...' because 'using' dispose 'stream' too!

      byte signature = reader.ReadByte();

      if (! Array.Exists(validSignatures, s => (s == signature)))
      {
        if (throwException)
        {
          throw ExceptionFactory.CreateArgumentOutOfRangeException("stream", "Signature of NTX index stream is invalid! '{0}'", signature);
        }

        return false;
      }


      byte signature2 = reader.ReadByte();

      if (signature2 != 0)
      {
        if (throwException)
        {
          throw ExceptionFactory.CreateArgumentOutOfRangeException("stream", "Second signature byte in NTX index stream header is invalid!");
        }

        return false;
      }

      //

      stream.Position = 12;

      UInt16 keySizePlus8 = reader.ReadUInt16();
      UInt16 keySize      = reader.ReadUInt16();

      if ((keySize < 1) || (keySize > 250))
      {
        if (throwException)
        {
          throw ExceptionFactory.CreateArgumentOutOfRangeException("stream", "Key size in NTX index stream header is invalid!");
        }

        return false;
      }

      if ((keySize + 8) != keySizePlus8)
      {
        if (throwException)
        {
          throw ExceptionFactory.CreateArgumentOutOfRangeException("stream", "Key size (+8) in NTX index stream header is invalid!");
        }

        return false;
      }

      //

      if (GetKeyExpression(stream) == null)
      {
        if (throwException)
        {
          throw ExceptionFactory.CreateArgumentOutOfRangeException("stream", "Key expression in NTX index stream header is invalid!");
        }

        return false;
      }

      byte uniqueFlag = reader.ReadByte();

      if (! ((uniqueFlag == 0) || (uniqueFlag == 1)))
      {
        if (throwException)
        {
          throw ExceptionFactory.CreateArgumentOutOfRangeException("stream", "Unique flag in NTX index stream header is invalid!");
        }

        return false;
      }

      // TODO: ...more check code here...

      return true;
    }


    static public byte[] GetKeyExpression(Stream stream)
    { // return full buffer, IndexFileBase calss will process it.                                    
      if (stream.Length < (pageSize * 2))
      {
        return null;                                                          // Null return value for signals key error!
      }

      //

      byte[] buffer = new byte[maxKeyLen];

      stream.Position = 22;                                                   // Position of "Key expression" in stream 
      stream.Read(buffer, 0, buffer.Length);

      return ProcessKeyExpressionBuffer(buffer);
    }

    private void FillHeader()
    {
      this.header = new NtxHeader();

      stream.Position = 0;   
 
      BinaryReader reader = new BinaryReader(stream);                           // don't use 'using (BinaryReader reader...' because 'using' dispose 'stream' too!

      this.header.signature = reader.ReadUInt16();
      this.header.version   = reader.ReadUInt16();
      this.header.root      = reader.ReadInt32();
      this.header.unused    = reader.ReadInt32();
      this.header.itemSize  = reader.ReadUInt16();
      this.header.keySize   = reader.ReadUInt16();
      this.header.keyDec    = reader.ReadUInt16();
      this.header.maxItem   = reader.ReadUInt16();
      this.header.halfPage  = reader.ReadUInt16();

      reader.BaseStream.Position = 278;
      this.header.unique    = reader.ReadBoolean();

      this.header.keyExpr   = Encoding.ASCII.GetString(GetKeyExpression(stream));
    }

    #endregion
  }

  //*******************************************************************************************************

  public class NtxHeader 
  {
    public UInt16   signature;                                                // must be equal to 03.
    public UInt16   version;                                                  // index system version.
    public Int32    root;                                                     // The first, or root page of an indes has a minimum of 1 entry regardless of this value.
    public Int32    unused;                                                   // first unused page offset.
    public UInt16   itemSize;                                                 // distance between keys in page / Key size + 8 bytes
    public UInt16   keySize;                                                  // key size.
    public UInt16   keyDec;                                                   // for numeric keys / No. of decimals in key                   
    public UInt16   maxItem;                                                  // The maximum number of keys (with their pointers) that can fit on an index page.
    public UInt16   halfPage;                                                 // The maximum number of keys that can fit on an index page, divided by two. This is an important value in a B-tree system as it is the minimum number of keys that must be on a page.
    public string   keyExpr;                                                  // The actual expression on which the index was built.
    public bool     unique;                                                   // Unique index flag
  };
}
