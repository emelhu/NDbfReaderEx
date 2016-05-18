using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace NDbfReaderEx
{
  public class MemoFileDBT : MemoFileBase
  {
    #region format parameters  ----------------------------------------------------------------------------
    private const byte  terminatorEOF      = 0x1A;
    private const byte  terminatorEOS      = 0x00;    
    
    public static MemoTerminators   defaultTerminators  = MemoTerminators.terminatorEOF;
    public        MemoTerminators   terminators         = defaultTerminators;

    public static StrictHeader      defaultStrictHeader = StrictHeader.medium;
    public        StrictHeader      strictHeader        = defaultStrictHeader;
                                      
    #endregion

    #region variables -------------------------------------------------------------------------------------

    private       int   blockSize;
    private object      lockObject      = new Object();

    public readonly MemoFileType memoType;

    #region MaxBlobSize
    /// <summary>
    /// Maximum size of readed/writed blob text in bytes (default value)
    /// </summary>
    public static int    maxBlobSizeDefault
    {
      get { return _maxBlobSizeDefault; }
      set
      {
        _maxBlobSizeDefault = value;

        if (_maxBlobSizeDefault < 64 * 1024)
        {
          _maxBlobSizeDefault   = 64 * 1024;
        }

        if (_maxBlobSizeDefault > 512 * 1024 * 1024)
        {
          _maxBlobSizeDefault   = 512 * 1024 * 1024;
        }

        if ((_maxBlobSizeDefault % 512) != 0)
        {
          _maxBlobSizeDefault = ((_maxBlobSizeDefault / 512) + 1) * 512;
        }
      }
    }           
    private static int   _maxBlobSizeDefault    = 64 * 1024 * 4;    
    
    /// <summary>
    /// Maximum size of readed/writed blob text in bytes 
    /// </summary>
    public static int    maxBlobSize
    {
      get { return _maxBlobSize; }
      set
      {
        _maxBlobSize = value;

        if (_maxBlobSize < 64 * 1024)
        {
          _maxBlobSize   = 64 * 1024;
        }

        if (_maxBlobSize > 512 * 1024 * 1024)
        {
          _maxBlobSize   = 512 * 1024 * 1024;
        }

        if ((_maxBlobSize % 512) != 0)
        {
          _maxBlobSize = ((_maxBlobSize / 512) + 1) * 512;
        }
      }
    }           
    private static int   _maxBlobSize    = _maxBlobSizeDefault;            
    #endregion

    #region memoBytesAllocateSize
    /// <summary>
    /// Allocate step size for read/write memo bytes (default value)
    /// </summary>
    public static int    memoBytesAllocateSizeDefault
    {
      get { return _memoBytesAllocateSizeDefault; }
      set
      {
        _memoBytesAllocateSizeDefault = value;

        if (_memoBytesAllocateSizeDefault < 4 * 1024)
        {
          _memoBytesAllocateSizeDefault   = 4 * 1024;
        }

        if (_memoBytesAllocateSizeDefault > 1024 * 1024)
        {
          _memoBytesAllocateSizeDefault   = 1024 * 1024;
        }

        if ((_memoBytesAllocateSizeDefault % 512) != 0)
        {
          _memoBytesAllocateSizeDefault = ((_memoBytesAllocateSizeDefault / 512) + 1) * 512;
        }
      }
    }           
    private static int   _memoBytesAllocateSizeDefault    = 16 * 1024;    
    
    /// <summary>
    /// Allocate step size for read/write memo bytes
    /// </summary>
    public static int    memoBytesAllocateSize
    {
      get { return _memoBytesAllocateSize; }
      set
      {
        _memoBytesAllocateSize = value;

        if (_memoBytesAllocateSize < 4 * 1024)
        {
          _memoBytesAllocateSize   = 4 * 1024;
        }

        if (_memoBytesAllocateSize > 1024 * 1024)
        {
          _memoBytesAllocateSize   = 1024 * 1024;
        }

        if ((_memoBytesAllocateSize % 512) != 0)
        {
          _memoBytesAllocateSize = ((_memoBytesAllocateSize / 512) + 1) * 512;
        }
      }
    }           
    private static int   _memoBytesAllocateSize    = _memoBytesAllocateSizeDefault;            
    #endregion
    #endregion

    public MemoFileDBT(Stream stream, Encoding encoding, MemoFileType memoType, StrictHeader? strictHeader)
      : base(stream, encoding)
    { // 'stream', 'encoding' already stored by base class constructor  
      if (memoType == MemoFileType.Undefined)
      {
        memoType = RetrieveDbtVersion(stream);
      }

      this.memoType = memoType;

      switch (memoType)
      {        
        case MemoFileType.DBT_Ver3:
          if (stream.Length < 512)
          {
            throw new Exception("DBT MemoStream length too short! [header ver3] [" + stream.Length + " < 512]");
          }
          break;

        case MemoFileType.DBT_Ver4:
          if (stream.Length < 64)
          {
            throw new Exception("DBT MemoStream length too short! [header ver4] [" + stream.Length + " < 64]");
          }
          break;

        case MemoFileType.Undefined:
          throw new Exception("MemoFileDBT: Undefined MemoFileType!");
        
        default:
          throw new Exception("MemoFileDBT: Invalid MemoFileType! [" + memoType + "]");
      }


      this.strictHeader = strictHeader ?? defaultStrictHeader;

      BinaryReader reader = new BinaryReader(stream);                           // don't use using '(BinaryReader reader...' because 'using' dispose 'stream' too!
      { // http://www.clicketyclick.dk/databases/xbase/format/dbt.html#DBT_STRUCT
        reader.BaseStream.Position = 16;
        byte versionByte = reader.ReadByte();             // dBase3:0x03   dBase4: 0x00

        reader.BaseStream.Position = 20;
        this.blockSize = reader.ReadInt16();              // dBase3:512

        if (this.strictHeader > StrictHeader.none)
        {
          switch (memoType)
          {
            case MemoFileType.DBT_Ver3:
              if ((versionByte != 0x03) && (this.strictHeader > StrictHeader.medium))
              {
                throw new Exception("MemoFileDBT: Invalid Memo File version byte! [" + versionByte + " is not 3]");
              }

              if (blockSize != 512) 
              {
                if (this.strictHeader > StrictHeader.medium)
                {
                  throw new Exception("MemoFileDBT: Invalid Memo File block size! [" + blockSize + " is not 512]");
                }

                blockSize = 512;
              }

              if ((this.strictHeader >= StrictHeader.weak) && (! CheckDbtFileBlocks(stream, blockSize)))
              {
                throw new Exception("MemoFileDBT: Invalid Memo File length! [" + stream.Length + "/" + blockSize + "]");
              }

              if ((this.strictHeader >= StrictHeader.potent) && (! CheckDbtFileAdmLength(stream)))
              {
                throw new Exception("MemoFileDBT: Invalid Memo File length! [next block]");
              }
              break;

            case MemoFileType.DBT_Ver4:
              if ((this.strictHeader >= StrictHeader.weak) && (versionByte != 0x00))
              {
                throw new Exception("MemoFileDBT: Invalid Memo File version byte! [" + versionByte + " is not 0]");
              }

              if ((blockSize < 64) || (blockSize > 64 * 512) || ((blockSize % 64) != 0))
              { // Can't read/write if blockSize unknown
                throw new Exception("MemoFileDBT: Invalid Memo File block size! [" + blockSize + " is not 512]");
              }

              if ((this.strictHeader >= StrictHeader.medium) && (! CheckDbtFileBlocks(stream, blockSize)))
              {
                throw new Exception("MemoFileDBT: Invalid Memo File length! [" + stream.Length + "/" + blockSize + "]");
              }

              if ((this.strictHeader >= StrictHeader.potent) && (! CheckDbtFileAdmLength(stream)))
              {
                throw new Exception("MemoFileDBT: Invalid Memo File length! [next block]");
              }
              break;

            default:
              Debug.Fail("invalid case");
              break;
          }
        }
      }

      stream.Position = 0;        
    }

    private bool CheckDbtFileAdmLength(Stream stream)
    {
      BinaryReader reader = new BinaryReader(stream);

      reader.BaseStream.Position = 0;
      int nextBlock = reader.ReadInt32();

      reader.BaseStream.Position = 20;
      int blockLength = reader.ReadInt16();           // dBase3:512

      long calculedPos  = nextBlock * blockLength;
      long streamLength = stream.Length;

      if (streamLength == (calculedPos + 1))
      {
        reader.BaseStream.Position = streamLength - 1;

        byte lastByte = reader.ReadByte();

        return (lastByte == 0x1F);                                    // EOF char enabled at some implementation
      }

      return (streamLength == calculedPos);
    }

    #region Interface ---------------------------------------------------------------------------------------

    /// <summary>
    /// Read content of memo field.
    /// (multithread calls enabled)
    /// </summary>
    /// <param name="blockNo">Pointer of first block in memo (DBT) file, readed from DBF's memo field.</param>
    /// <returns></returns>
    public override byte[] ReadMemoBytes(int blockNo)
    {
      byte[] retBytes = null;

      if (blockNo < 1)
      {
        throw ExceptionFactory.CreateArgumentException("blockNo", "ReadMemoBytes({0}) invalid block number!", blockNo);
      }

      if ((blockNo * blockSize) >= stream.Length)
      {
        throw ExceptionFactory.CreateArgumentException("blockNo", "ReadMemoBytes({0}) out of dbt stream length!", blockNo);
      }

      lock (lockObject)                                                                  
      {
        switch (memoType)
        {
          case MemoFileType.DBT_Ver3:
            retBytes = ReadMemoArray3(blockNo);
            break;
          case MemoFileType.DBT_Ver4:
            retBytes = ReadMemoArray4(blockNo);
            break;
          default:
            throw new Exception("MemoFileDBT/ReadMemoBytes: invalid switch case!");
        }
      }

      return retBytes;
    }

    public override int WriteMemoBytes(byte[] newBytes, int oldBlockNo = 0)
    {
      throw new NotImplementedException();
    }
    #endregion

    #region Help functions ----------------------------------------------------------------------------------

    private byte[] ReadMemoArray4(int blockNo)
    { // return content of memo in 'memoBytes' and size of useful bytes
      byte[] memoBytes = null;

      BinaryReader reader = new BinaryReader(stream);                           // don't use using '(BinaryReader reader...' because 'using' dispose 'stream' too!
      reader.BaseStream.Position = (blockNo * blockSize);

      byte[] signal = reader.ReadBytes(4);        

      if (isDbt4MemoBlockSignal(signal))
      {
        int memoLen = reader.ReadInt32();

        if ((memoLen < 1) || (memoLen > maxBlobSize))
        {
          throw new Exception(String.Format("ReadMemoArray4({0}): memo signal OK, but memo length error! [{1}/1..{2}]", blockNo, memoLen, maxBlobSize));
        }

        memoBytes = reader.ReadBytes(memoLen);    
      }
      else
      {
        throw ExceptionFactory.CreateArgumentException("blockNo", "ReadMemoArray4({0}) invalid block number or wrong fileformat or file corrupted!", blockNo);
      }

      return memoBytes;
    }

    private byte[] ReadMemoArray3(int blockNo)
    { // return content of memo in 'memoBytes' and size of useful bytes
      int    retLen    = 0;
      byte[] memoBytes = new byte[memoBytesAllocateSize];

      stream.Position = (blockNo * blockSize);

      for (int blockLoop = 0; (blockLoop <= (maxBlobSize - blockSize)); blockLoop += blockSize)
      { // blockLoop: start of read to buffer, read length is blockSize
        int readed = stream.Read(memoBytes, blockLoop, blockSize);

        //if (readed < blockSize)     -- it's no problem! When you read last record of memo file Read() result smaller block then blockSize.
        //{
        //  throw ExceptionFactory.CreateArgumentException("blockNo", "Read {0}. block of dbt memo stream: end of stream (read a segment from {1}. byte of block)!", blockNo, blockLoop);
        //}

        int terminatorPos = int.MinValue;

        switch (terminators)
        {
          case MemoTerminators.both:
            {
              int terminatorPosEOF = Array.IndexOf(memoBytes, terminatorEOF);                  // standard dBase3 terminator
              int terminatorPosEOS = Array.IndexOf(memoBytes, terminatorEOS);                  // special terminator

              if (terminatorPosEOF < 0)
              {
                terminatorPos = terminatorPosEOS;
              }
              else if (terminatorPosEOS < 0)
              {
                terminatorPos = terminatorPosEOF;
              }
              else
              {
                terminatorPos = Math.Min(terminatorPosEOF, terminatorPosEOS);
              }
            }
            break;
          case MemoTerminators.terminatorEOF:
            terminatorPos = Array.IndexOf(memoBytes, terminatorEOF);                  // standard dBase3 terminator
            break;
          case MemoTerminators.terminatorEOS:
            terminatorPos = Array.IndexOf(memoBytes, terminatorEOS);                  // BDE (dbase7) terminator
            break;
          default:
            break;
        }

        if (terminatorPos >= 0)
        { // Found end of memo text
          retLen = terminatorPos;
          break;                                                                    // end of loop, return value found
        }
        else
        { // maybe terminated by loop (reaches 'maxBlobSize' value)
          retLen = (blockLoop + blockSize);                                            // actually used buffer length
        }

        int calculedSize = (blockLoop + 3) * blockSize; 
        if (calculedSize > memoBytes.Length)                                     
        { // if not enought space for next read.
          int allocateSize = Math.Min(memoBytes.Length * 2, maxBlobSize);
          allocateSize     = Math.Max(allocateSize, calculedSize);

          Array.Resize<byte>(ref memoBytes, allocateSize);
        }
      }   
      
      if ((memoBytes.Length - retLen) > (memoBytes.Length / 10))
      { // more than 10% loss
        byte[] tempBytes = new byte[retLen];
        Array.Copy(memoBytes, tempBytes, retLen);  
        memoBytes = tempBytes;
      }
      else
      {     
        Array.Resize<byte>(ref memoBytes, retLen);
      }
      
      return memoBytes;
    }

    public MemoFileType RetrieveDbtVersion(Stream stream)
    {
      if (stream.Length < 64)
      { // http://www.clicketyclick.dk/databases/xbase/format/dbt.html#DBT_STRUCT --> different rule from BDE test data !
        throw new Exception("RetrieveDbtVersion: DBT MemoStream length too short! [" + stream.Length + " < 64]");
      }

      BinaryReader reader = new BinaryReader(stream);   

      reader.BaseStream.Position = 16;
      byte versionByte = reader.ReadByte();                                   // dBase3:0x03   dBase4: 0x00

      reader.BaseStream.Position = 20;
      int blockLength = reader.ReadInt16();                                   // dBase3:512, dBase4: (1-512)*54  http://www.dbase.com/help/IDE/IDH_IDE_SET_MBLOCK.htm

      if ((blockLength % 64) != 0)
      {
        throw new Exception("RetrieveDbtVersion: DBT MemoStream blockLength isn't a multiply of 64 !");
      }

      if (! CheckDbtFileBlocks(stream, blockLength))
      {
        throw new Exception("RetrieveDbtVersion: DBT MemoStream length isn't a multiply of " + blockLength + " !");
      }

      if ((versionByte == 0x03) && (blockLength == 512))
      {
        return MemoFileType.DBT_Ver3;                                       // certainly not DBT_Ver4, and very likely DBT_Ver3
      }

      if (blockLength != 512)
      {
        return MemoFileType.DBT_Ver4;                                       // maybe DBT_Ver4
      }

      bool ver4Sign = (SeekDbt4FileMemoBlock(stream, blockLength) > 0);

      if (versionByte == 0x00)
      {
        if (ver4Sign)
        {
          return MemoFileType.DBT_Ver4;                                     // certainly DBT_Ver4
        }

        return MemoFileType.DBT_Ver4;                                       // maybe DBT_Ver4
      }
      else if (ver4Sign)
      {
        return MemoFileType.DBT_Ver4;                                       // certainly DBT_Ver4
      }
 

      return MemoFileType.DBT_Ver3;                                         // maybe: DBT_Ver3 files can contain garbage in head without any problems
    }

    private bool CheckDbtFileBlocks(Stream stream, int blockSize)
    {
      if (blockSize < 64)
      {
        return false;
      }

      if ((blockSize % 64) != 0)
      {
        return false;
      }

      if (stream.Length < blockSize)
      {
        return false;
      }
        
      long mod = (stream.Length % blockSize);

      if (mod == 0)
      {
        return true;
      }
      else if (mod == 1)
      {
        stream.Position = stream.Length - 1;
        BinaryReader reader = new BinaryReader(stream); 

        byte last = reader.ReadByte();

        return (last == 0x1A);                                    // EOF marker byte (there is example file with this format)
      }

      return false;
    }

    /// <summary>
    /// Return starting position of first signal block (0 if haven't any)
    /// </summary>
    /// <param name="stream"></param>
    /// <param name="blockSize"></param>
    /// <returns></returns>
    private int SeekDbt4FileMemoBlock(Stream stream, int blockSize)
    {
      stream.Position = 0;
      BinaryReader reader = new BinaryReader(stream);

      if ((blockSize % 1024) == 0)
      { // BDE checked fileformat
        for (int i = blockSize; ((i + 8) < stream.Length); i += blockSize)
        {
          reader.BaseStream.Position = i;

          if (isDbt4MemoBlockSignal(reader.ReadBytes(4)))
          {
            return i;
          }
        }
      }

      for (int i = 512; ((i + 8) < stream.Length); i += blockSize)
      { // Standard?  Need test files :((    http://www.clicketyclick.dk/databases/xbase/format/dbt.html#DBT_STRUCT
        reader.BaseStream.Position = i;

        if (isDbt4MemoBlockSignal(reader.ReadBytes(4)))
        {
          return i;
        }
      }

      return 0;
    }

    private bool isDbt4MemoBlockSignal(byte[] signal)
    {
      if (signal.Length != 4)
      {
        return false;
      }

      if (signal[0] != 0xFF)
      {
        return false;
      }

      if (signal[1] != 0xFF)
      {
        return false;
      }

      if (signal[2] != 0x08)
      {
        return false;
      }

      if (signal[3] != 0x00)
      {
        return false;
      }

      return true;
    }
    #endregion
  }  
}
