namespace NDbfReaderEx
{
  /// <summary>
  /// Supported native dBASE column types.
  /// </summary>
  public enum NativeColumnType : byte
  {
    /// <summary>
    /// Characters - padded with blanks to the width of the field. 
    /// </summary>
    /// <remarks>C in ASCII</remarks>
    Char = 0x43,

    /// <summary>
    /// Memo (text blob) - stored separately in other datafile. 
    /// </summary>
    /// <remarks>M in ASCII</remarks>
    Memo = 0x4D,

    /// <summary>
    /// 8 bytes - date stored as a string in the format YYYYMMDD.
    /// </summary>
    /// <remarks>D in ASCII</remarks>
    Date = 0x44,

    /// <summary>
    /// 4 bytes. Leftmost bit used to indicate sign, 0 negative.
    /// </summary>
    /// <remarks> I in ASCII</remarks>
    Long = 0x49,

    /// <summary>
    /// 1 byte - initialized to 0x20 (space) otherwise T or F
    /// </summary>
    /// <remarks>L in ASCII</remarks>
    Logical = 0x4C,

    /// <summary>
    /// Number stored as a string, right justified, and padded with blanks to the width of the field. 
    /// </summary>
    /// <remarks>N in ASCII</remarks>
    Numeric = 0x4E,

    /// <summary>
    /// Number stored as a string, right justified, and padded with blanks to the width of the field. 
    /// </summary>
    /// <remarks>F in ASCII</remarks>
    Float = 0x46,

    /// <summary>
    /// Number stored as a double (8 byte binary value) 
    /// </summary>
    /// <remarks>O in ASCII</remarks>
    Double = 0x4F
  }
}
