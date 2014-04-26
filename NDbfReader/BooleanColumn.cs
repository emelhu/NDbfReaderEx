using System;
using System.Diagnostics;
using System.Globalization;
using System.Text;

namespace NDbfReader
{
  /// <summary>
  /// Represents a <see cref="Boolean"/> column.
  /// </summary>
  [DebuggerDisplay("Boolean {Name}")]
  public class BooleanColumn : Column<bool?>
  {
    /// <summary>
    /// Initializes a new instance with the specified name and offset.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="offset">The column offset in a row in bytes.</param>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is &lt; 0.</exception>
    public BooleanColumn(string name, byte dbfType, int offset)
      : base(name, dbfType, offset, 1, 0, null)                                                   // fix size: 1
    {
    }

    /// <summary>
    /// Loads a value from the specified buffer.
    /// </summary>
    /// <param name="buffer">The byte array from which a value should be loaded. The buffer length is always at least equal to the column size.</param>
    /// <param name="encoding">The encoding that should be used when loading a value. The encoding is never <c>null</c>.</param>
    /// <returns>A column value.</returns>
    protected override bool? ValueFromRowBuffer(byte[] rowBuffer)
    {
      var charValue = Char.ToUpper((char)rowBuffer[_offset], CultureInfo.InvariantCulture);

      switch (charValue)
      {
        case 'T':
        case 'Y':
          return true;
        case 'F':
        case 'N':
          return false;
        default:
          return null;
      }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="_buffer"></param>
    /// <returns></returns>
    public override bool IsNull(byte[] _buffer)
    {
      return false;                                                 // TODO: !check conditions!
    }
  }
}
