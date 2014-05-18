using System;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;

namespace NDbfReaderEx
{
  /// <summary>
  /// Represents a <see cref="Decimal"/> column.
  /// </summary>
  [DebuggerDisplay("Decimal {Name}")]
  public class DecimalColumn : Column<decimal>
  {
    private static readonly NumberFormatInfo DecimalNumberFormat = new NumberFormatInfo() { NumberDecimalSeparator = "." };

    /// <summary>
    /// Initializes a new instance with the specified name and offset.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="offset">The column offset in a row in bytes.</param>
    /// <param name="size">The column size in bytes.</param>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is &lt; 0 or <paramref name="size"/> is &lt; 0.</exception>
    public DecimalColumn(string name, NativeColumnType dbfType, int offset, short size, short dec)
      : base(name, dbfType, offset, size, dec, null)
    {
    }

    /// <summary>
    /// Loads a value from the specified buffer.
    /// </summary>
    /// <param name="buffer">The byte array from which a value should be loaded. The buffer length is always at least equal to the column size.</param>
    /// <returns>A column value.</returns>
    protected override decimal ValueFromRowBuffer(byte[] rowBuffer, ref byte[] cachedColumnData)
    { // This didn't use cachedColumnData, it for MemoColumn only
      if (IsNull(rowBuffer))
      {
        return 0;
      }

      var stringValue = Encoding.ASCII.GetString(rowBuffer, offset_ + 1, size_); 

      return decimal.Parse(stringValue, NumberStyles.Float | NumberStyles.AllowLeadingWhite, DecimalNumberFormat);
    }

    public override bool IsNull(byte[] rowBuffer)
    {
      for (int i = 0; i < size_; i++)
      {
        byte b = rowBuffer[offset_ + 1 + i];

        if (b == 0x00)
        { // not standard, but maybe a C/C++ EndOfString character used
          break;
        }

        if (b == 0x3F)
        { // '?' character found
          break;
        }

        if (b != 0x20)
        { // if contains any non blank character it isn't null value
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
