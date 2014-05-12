using System;
using System.Diagnostics;
using System.Text;

namespace NDbfReaderEx
{
  /// <summary>
  /// Represents a <see cref="String"/> column.
  /// </summary>
  [DebuggerDisplay("String {Name}")]
  public class StringColumn : Column<string>
  {
    /// <summary>
    /// Initializes a new instance with the specified name and offset.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="offset">The column offset in a row in bytes.</param>
    /// <param name="size">The column size in bytes.</param>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is &lt; 0 or <paramref name="size"/> is &lt; 0.</exception>
    public StringColumn(string name, NativeColumnType dbfType, int offset, short size, short dec, Encoding encoding)
      : base(name, dbfType, offset, size, dec, encoding)
    {
    }

    /// <summary>
    /// Loads a value from the specified buffer.
    /// </summary>
    /// <param name="buffer">The byte array from which a value should be loaded. The buffer length is always at least equal to the column size.</param>
    /// <param name="encoding">The encoding that should be used when loading a value. The encoding is never <c>null</c>.</param>
    /// <returns>A column value.</returns>
    protected override string ValueFromRowBuffer(byte[] rowBuffer, ref byte[] cachedColumnData)
    { // This didn't use cachedColumnData, it for MemoColumn only
      var ret = encoding_.GetString(rowBuffer, offset_ + 1, size_);             // encoding_: for converting by local character set

      int endPos = ret.IndexOf('\0');
      if (endPos >= 0)
      {
        ret = ret.Substring(0, endPos);
      }

      return ret.TrimEnd();                                                            
    }

    public override bool IsNull(byte[] rowBuffer)
    {
      return (rowBuffer[offset_ + 1] == 0x00);                                   // This is not standard, but very logical ;)   [practically always returns false]
    }
  }
}
