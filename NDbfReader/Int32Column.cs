using System;
using System.Diagnostics;
using System.Text;

namespace NDbfReader
{
  /// <summary>
  /// Represents a <see cref="Int32"/> column.
  /// </summary>
  [DebuggerDisplay("Int32 {Name}")]
  public class Int32Column : Column<int>
  {
    /// <summary>
    /// Initializes a new instance with the specified name and offset.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="offset">The column offset in a row in bytes.</param>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is &lt; 0.</exception>
    public Int32Column(string name, byte dbfType, int offset)
      : base(name, dbfType, offset, 4, 0, null)
    {
    }

    /// <summary>
    /// Loads a value from the specified buffer.
    /// </summary>
    /// <param name="buffer">The byte array from which a value should be loaded. The buffer length is always at least equal to the column size.</param>
    /// <param name="encoding">The encoding that should be used when loading a value. The encoding is never <c>null</c>.</param>
    /// <returns>A column value.</returns>
    protected override int ValueFromRowBuffer(byte[] rowBuffer)
    {
      return BitConverter.ToInt32(rowBuffer, _offset);
    }

    public override bool IsNull(byte[] _buffer)
    {
      return false;                                                 // TODO: !check conditions!
    }
  }
}
