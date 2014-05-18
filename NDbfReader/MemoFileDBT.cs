using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace NDbfReaderEx
{
  internal class MemoFileDBT : MemoFileBase
  {

    # region variables --------------------------------------------------------------------------------------

    public  const int   blockSize       = 512;
    private const byte  terminator      = 0x1A;
    private const uint  defaultBlobSize = 64 * 1024;                          // 64 KByte - default blob max. size (like dBase/Clipper)

    private static uint _maxBlobSize    = defaultBlobSize;                                              

    private object      lockObject      = new Object();

    private byte[]      memoBytes       = new byte[blockSize * 8];            // for decrease allocates; it will free with memo file class   

    #endregion

    public MemoFileDBT(Stream stream, Encoding encoding)
      : base(stream, encoding)
    { // 'stream', 'encoding' already stored by base class constructor    
    }

    #region Interface ---------------------------------------------------------------------------------------
    
    /// <summary>
    /// Maximum size of readed/writed blob text in bytes (default as dBase3/Clipper, 64K)
    /// </summary>
    public static uint maxBlobSize
    {
      get {return _maxBlobSize; }

      set
      {
        uint size = (value % blockSize) * blockSize;

        if (size < value)
        {
          size += blockSize;
        }

        _maxBlobSize = Math.Max(Math.Min(size, 1024*1024), defaultBlobSize);
      }
    }
   
    /// <summary>
    /// Read content of memo field.
    /// (multithread calls enabled)
    /// </summary>
    /// <param name="blockNo">Pointer of first block in memo (DBT) file, readed from DBF's memo field.</param>
    /// <returns></returns>
    public override byte[] ReadMemoBytes(int blockNo)
    {
      lock (lockObject)                                                                  
      {
        int size = ReadMemoArray(blockNo);

        byte[] retBytes = new byte[size];

        Array.Copy(memoBytes, retBytes, size);

        return retBytes;
      }
    }

    public override int WriteMemoBytes(byte[] newBytes, int oldBlockNo = 0)
    {
      throw new NotImplementedException();
    }
    #endregion

    #region Help functions ----------------------------------------------------------------------------------

    private int ReadMemoArray(int blockNo)
    { // return content of memo in 'memoBytes' and size of useful bytes
      if (blockNo < 1)
      {
        throw ExceptionFactory.CreateArgumentException("blockNo", "ReadMemoArray({0}) invalid block number!", blockNo);
      }

      if ((blockNo * blockSize) > stream.Length)
      {
        throw ExceptionFactory.CreateArgumentException("blockNo", "ReadMemoArray({0}) out of dbt stream length!", blockNo);
      }

      //

      int ret = 0;

      stream.Position = (blockNo * blockSize);

      for (int blockLoop = 0; (blockLoop <= (maxBlobSize - blockSize)); blockLoop += blockSize)
      { // blockLoop: start of read to buffer, read length is blockSize
        int readed = stream.Read(memoBytes, blockLoop, blockSize);

        if (readed < blockSize)
        {
          throw ExceptionFactory.CreateArgumentException("blockNo", "Read {0}. block of dbt memo stream: end of stream (read a segment from {1}. byte of block)!", blockNo, blockLoop);
        }

        int terminatorPos = Array.IndexOf(memoBytes, terminator);

        if (terminatorPos >= 0)
        { // Found end of memo text
          ret = terminatorPos;
          break;                                                                    // end of loop, return value found
        }
        else
        { // maybe terminated by loop (reaches 'maxBlobSize' value)
          ret = (blockLoop + blockSize);                                            // actually used buffer length
        }

        if ((blockLoop + (2 * blockSize)) > memoBytes.Length)                                     
        { // if not enought space for next read.
          int allocateSize = Math.Min(memoBytes.Length * 2, (int)maxBlobSize);

          Array.Resize<byte>(ref memoBytes, allocateSize);
        }
      }      

      return ret;
    }
    #endregion
  }
}
