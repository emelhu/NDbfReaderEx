using System;

namespace NDbfReaderEx
{
  /// <summary>
  /// Represents only basic information of dBASE/Clipper column. 
  /// It is targeted for store reader's header info and for create new table
  /// </summary>
  public interface IColumnBase
  {
    /// <summary>
    /// Gets the column name.
    /// </summary>
    string name { get; }

    /// <summary>
    /// Gets the <c>DBF</c> type of a column value.
    /// </summary>
    NativeColumnType dbfType { get; }

    /// <summary>
    /// Gets the column size in bytes/characters.
    /// </summary>
    short size { get; }

    /// <summary>
    /// Gets the number of decimal places in bytes/characters.
    /// </summary>
    short dec { get; }
  }


  /// <summary>
  /// Represents a dBASE/Clipper column.
  /// </summary>
  public interface IColumn : IColumnBase
  {
    /// <summary>
    /// Gets the <c>CLR</c> type of a column value.
    /// </summary>
    Type type { get; }

    /// <summary>
    /// Gets the width to display of a column value.
    /// If column type is memo, returns 0 because it is variable width.
    /// </summary>
    int displayWidth { get; }

    /// <summary>
    /// Better side to display of a column value.
    /// </summary>
    bool leftSideDisplay { get; }                 
  }
}
