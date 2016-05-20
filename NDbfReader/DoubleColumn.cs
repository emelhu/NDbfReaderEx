using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace NDbfReaderEx
{
  /// <summary>
  /// Represents a <see cref="Double"/> column.
  /// </summary>
  [DebuggerDisplay("Double {Name}")]
  public class DoubleColumn : Column<Double>
  {
    /// <summary>
    /// Initializes a new instance with the specified name and offset.
    /// </summary>
    /// <param name="name">The column name.</param>
    /// <param name="offset">The column offset in a row in bytes.</param>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is &lt; 0.</exception>
    public DoubleColumn(string name, NativeColumnType dbfType, int offset)
      : base(name, dbfType, offset, 8, 0, null)
    {
    }

    /// <summary>
    /// Loads a value from the specified buffer.
    /// </summary>
    /// <param name="buffer">The byte array from which a value should be loaded. The buffer length is always at least equal to the column size.</param>
    /// <param name="encoding">The encoding that should be used when loading a value. The encoding is never <c>null</c>.</param>
    /// <returns>A column value.</returns>
    protected override double ValueFromRowBuffer(byte[] rowBuffer, ref byte[] cachedColumnData)
    { // This didn't use cachedColumnData, it for MemoColumn only
      byte[] doubleBuff = new byte[8];
      byte[] doubleDBF  = new byte[doubleBuff.Length];

      Array.Copy(rowBuffer, offset_ + 1, doubleDBF, 0, doubleDBF.Length);      

      doubleBuff[0] = doubleDBF[7];                                           // Bigendian / littleendian byte order correction
      doubleBuff[1] = doubleDBF[6];
      doubleBuff[2] = doubleDBF[5];
      doubleBuff[3] = doubleDBF[4];
      doubleBuff[4] = doubleDBF[3];
      doubleBuff[5] = doubleDBF[2];
      doubleBuff[6] = doubleDBF[1];
      doubleBuff[7] = doubleDBF[0];
      
      double ret = BitConverter.ToDouble(doubleBuff, 0);

      // TODO: Corrections! Positive numbers OK, if I change sign!  Negative numbers WRONG!

      return ret * -1;
    }
    // Prior to the IEEE floating point standard being widely accepted, 
    // Microsoft used an internal floating point format known as Microsoft Binary Format (MBF). 
    // The IEEE standard was introduced later and became the industry standard. 
    // http://www.delphigroups.info/2/51/78909.html
    // http://community.embarcadero.com/article/technical-articles/162-programming/14799-converting-between-microsoft-binary-and-ieee-forma
    // http://www.codingtiger.com/questions/bitwise/Convert-MBF-Double-to-IEEE.html
    // http://support.microsoft.com/kb/140520    MBF --> IEEE
    // http://www.wow.com/wiki/Microsoft_Binary_Format
    // http://support.microsoft.com/kb/35826:
    // MBF double-precision values are stored in the following format:
    //  -------------------------------------------------
    // |              |    |                             |
    // |8 Bit Exponent|Sign|   55 Bit Mantissa           |
    // |              | Bit|                             |
    //  -------------------------------------------------
    // IEEE double precision values are stored in the following format:
    //  -------------------------------------------------
    // |    |                | |                         |
    // |Sign| 11 Bit Exponent|1|  52 Bit Mantissa        |
    // | Bit|                | |                         |
    //  -------------------------------------------------
    //                        ^
    //                        Implied Bit (always 1)

    // According to an old MASM 5.0 programmer's guide, there was a Microsoft Binary format for encoding real numbers, both short (32 bits) and long (64 bits).
    // There were 3 parts:
    // Biased 8-bit exponent in the highest byte (last in the little-endian view we've been using) It says the bias is 0x81 for short numbers and 0x401 for long, but I'm not sure where that lines up. I just got there by experimentation.
    // Sign bit (0 for +, 1 for -) in upper bit of second highest byte.
    // All except the first set bit of the mantissa in the remaining 7 bits of the second highest byte, and the rest of the bytes. And since the most signficant bit for non-zero numbers is 1, it is not represented. But if if were, it would share the same bit position where the sign is (that's why I or-ed it in there to complete the actual mantissa).

    public override bool IsNull(byte[] _buffer)
    {
      return false;                                               
    }

    public override void SetNull(byte[] rowBuffer)
    {
      for (int i = 0; i < size_; i++)
      {
        rowBuffer[offset_ + 1 + i] = 0x00;     
      }
    }
  }
}
