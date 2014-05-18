using System;
using System.Diagnostics;
using System.Globalization;
using System.Text;

namespace NDbfReaderEx
{
  /// <summary>
  /// Represents a <see cref="Boolean"/> column.
  /// </summary>
  [DebuggerDisplay("Boolean {Name}")]
  public class BooleanColumn : Column<bool>
  {
    /// <summary>
    /// Initializes a new instance with the specified name and offset.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="offset">The column offset in a row in bytes.</param>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is &lt; 0.</exception>
    public BooleanColumn(string name, NativeColumnType dbfType, int offset)
      : base(name, dbfType, offset, 1, 0, null)                                                   // fix size: 1
    {
    }

    /// <summary>
    /// Loads a value from the specified buffer.
    /// </summary>
    /// <param name="buffer">The byte array from which a value should be loaded. The buffer length is always at least equal to the column size.</param>
    /// <param name="encoding">The encoding that should be used when loading a value. The encoding is never <c>null</c>.</param>
    /// <returns>A column value.</returns>
    protected override bool ValueFromRowBuffer(byte[] rowBuffer, ref byte[] cachedColumnData)
    { // This didn't use cachedColumnData, it for MemoColumn only
      byte code = rowBuffer[offset_ + 1];

      switch (code)
      {
        case 0x54:                                                      // 'T'
        case 0x74:                                                      // 't'
        case 0x59:                                                      // 'Y'       
        case 0x79:                                                      // 'y'
          return true;
        case 0x46:                                                      // 'F'
        case 0x66:                                                      // 'f'
        case 0x4E:                                                      // 'N' 
        case 0x6E:                                                      // 'n' 
          return false;
        case 0x20:                                                      // ' '
        case 0x3F:                                                      // '?'
          return false;
        default:
          throw ExceptionFactory.CreateArgumentOutOfRangeException(this.name, "Invalid boolean character: '{0}'", code);
      }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="_buffer"></param>
    /// <returns></returns>
    public override bool IsNull(byte[] rowBuffer)
    {
      switch (rowBuffer[offset_ + 1])
      {
        case 0x20:                                                      // ' '
          return true;
        case 0x54:                                                      // 'T'
        case 0x74:                                                      // 't'
        case 0x59:                                                      // 'Y'
        case 0x79:                                                      // 'y'
        case 0x46:                                                      // 'F'
        case 0x66:                                                      // 'f'
        case 0x4E:                                                      // 'N'
        case 0x6E:                                                      // 'n':
          return false;
        default:
          return true;
      }
    }

    public override void SetNull(byte[] rowBuffer)
    {
      rowBuffer[offset_ + 1] = 0x20;     
    }
  }
}
