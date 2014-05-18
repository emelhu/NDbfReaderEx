using System;
using System.Diagnostics;
using System.IO;
using System.Text;

namespace NDbfReaderEx
{
  /// <summary>
  /// Represents a <see cref="String"/> column.
  /// </summary>
  [DebuggerDisplay("String {Name}")]
  public class MemoColumn : Column<string>
  {
    /// <summary>
    /// Initializes a new instance with the specified name and offset.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="offset">The column offset in a row in bytes.</param>
    /// <param name="size">The column size in bytes.</param>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is &lt; 0 or <paramref name="size"/> is &lt; 0.</exception>
    public MemoColumn(string name, NativeColumnType dbfType, int offset, short size, short dec, Encoding encoding)
      : base(name, dbfType, offset, size, dec, encoding)
    {
    }

    internal IMemoFile memoFile = null;                                          // set by DbfTable when stream of memo specified.

    /// <summary>
    /// Program can control of usage
    /// </summary>
    public static bool exceptionIfNoMemoStream = true;                            

    /// <summary>
    /// Loads a value from the specified buffer and seek real memo content from separated memo stream.
    /// </summary>
    /// <param name="buffer">The byte array from which a value should be loaded. The buffer length is always at least equal to the column size.</param>
    /// <param name="encoding">The encoding that should be used when loading a value. The encoding is never <c>null</c>.</param>
    /// <returns>A column text value, but null value signal if don't read from memo stream.</returns>
    protected override string ValueFromRowBuffer(byte[] rowBuffer, ref byte[] cachedColumnData)
    {
      if (cachedColumnData == null)
      { // cachedColumnData for decrease stream read
        if (IsNull(rowBuffer))
        {
          return String.Empty;
        }


        if (memoFile == null)
        {
          if (exceptionIfNoMemoStream)
          {
            throw ExceptionFactory.CreateNotSupportedException("memoFile", "Stream of memo text is not defined!");
          }
          else
          {
            return null;
          }
        }
        else if (memoFile.disposed)
        {
          if (exceptionIfNoMemoStream)
          {
            throw ExceptionFactory.CreateNotSupportedException("memoFile", "Stream of memo text is disposed!");
          }
          else
          {
            return null;
          }
        }
        else
        {
          string temp = Encoding.ASCII.GetString(rowBuffer, offset_ + 1, size_);

          int endPos = temp.IndexOf('\0');
          if (endPos >= 0)
          {
            temp = temp.Substring(0, endPos);
          }

          int memoIndex;

          if (!int.TryParse(temp, out memoIndex))
          {
            throw ExceptionFactory.CreateNotSupportedException("memoIndex", "Content of memo field '{0}' invalid (It isn't a block number)!", temp);
          }

          cachedColumnData = memoFile.ReadMemoBytes(memoIndex);
        }
      }

      return encoding_.GetString(cachedColumnData);                                              
    }

    public override bool IsNull(byte[] rowBuffer)
    {
      for (int i = 0; i < size_; i++)
      {
        int pos = offset_ + 1 + i;

        if ((rowBuffer[pos] != 0x00) && (rowBuffer[pos] != 0x20))
        {
          return false;
        }        
      }

      return true;
    }

    public override void SetNull(byte[] rowBuffer)
    {
      for (int i = 0; i < size_; i++)
      {
        rowBuffer[offset_ + 1 + i] = 0x20;     
      }
    }
  }
}
