using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;

namespace NDbfReaderEx
{
  /// <summary>
  /// Represent one row of DBF file. It is readable in detached mode too.
  /// Content of record is modifiable, but you have to write back to DbfTable, when complet.
  /// </summary>
  public class DbfRow
  {
    internal readonly IColumn[] _columns;
    internal readonly byte[]    _buffer;

    internal          int       _recNo;                                                                 // internal: DbfReaded/InsertRow() can modify it

    internal          bool      _modified = false;                                                      // internal: DbfRead/UpdateRow() can clear it

    private const byte DELETED_ROW_FLAG  = (byte)'*';
    private const byte ACCEPTED_ROW_FLAG = 0x20;                                                        // blank - normal/live data (undeleted)                                        

    /// <summary>
    /// Contructor of row.
    /// </summary>
    /// <param name="recNo">No. of row in dbf file (first is 0)</param>
    /// <param name="buffer">bytes of entire record content</param>
    /// <param name="columns">DbfTable header information for detached mode</param>
    protected internal DbfRow(int recNo, byte[] buffer, IColumn[] columns)
    {
      this._columns = columns;
      this._buffer  = buffer;
      this._recNo   = recNo;
    }

    #region Record status/info ----------------------------------------------------------------------------
    
    public bool deleted                                                                 // syntax like dBase
    {
      get
      {
        return (_buffer[0] == DELETED_ROW_FLAG);                                        // signal of deleted state
      }

      set
      {
        byte marker = value ? DELETED_ROW_FLAG : ACCEPTED_ROW_FLAG; 

        if (_buffer[0] != marker)
        {
          _modified  = true;
          _buffer[0] = marker;                                 
        }        
      }
    }


    public bool modified
    {
      get
      {
        return _modified;                                         
      }
    }


    public int recNo 
    { 
      get 
      { 
        return _recNo; 
      } 
    }
    #endregion

    #region field read --------------------------------------------------------------------------------------

    /// <summary>
    /// Gets a <see cref="String"/> value of the specified column of the current row.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    /// <returns>A <see cref="String"/> value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="columnName"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// No column with this name was found.<br />
    /// -- or --<br />
    /// The column has different type then <see cref="String"/>.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
    /// -- or --<br />
    /// The underlying stream is non-seekable and columns are read out of order.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
    public virtual string GetString(string columnName)
    {
      return GetValue<string>(columnName);
    }

    /// <summary>
    /// Gets a <see cref="String"/> value of the specified column of the current row.
    /// </summary>
    /// <param name="column">The column.</param>
    /// <returns>A <see cref="String"/> value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="column"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The column has different type then <see cref="String"/>.<br />
    /// -- or --<br />
    /// The column is from different table instance.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
    /// -- or --<br />
    /// The underlying stream is non-seekable and columns are read out of order.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
    public virtual string GetString(IColumn column)
    {
      CheckColumn(column);

      return GetValue<string>(column);
    }

    /// <summary>
    /// Gets a <see cref="Decimal"/> value of the specified column of the current row.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    /// <returns>A <see cref="Decimal"/> value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="columnName"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// No column with this name was found.<br />
    /// -- or --<br />
    /// The column has different type then <see cref="Decimal"/>.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
    /// -- or --<br />
    /// The underlying stream is non-seekable and columns are read out of order.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
    public virtual decimal GetDecimal(string columnName)
    {
      return GetValue<decimal>(columnName);
    }

    /// <summary>
    /// Gets a <see cref="Decimal"/> value of the specified column of the current row.
    /// </summary>
    /// <param name="column">The column.</param>
    /// <returns>A <see cref="Decimal"/> value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="column"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The column has different type then <see cref="Decimal"/>.<br />
    /// -- or --<br />
    /// The column is from different table instance.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
    /// -- or --<br />
    /// The underlying stream is non-seekable and columns are read out of order.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
    public virtual decimal GetDecimal(IColumn column)
    {
      CheckColumn(column);

      return GetValue<decimal>(column);
    }

    /// <summary>
    /// Gets a <see cref="DateTime"/> value of the specified column of the current row.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    /// <returns>A <see cref="DateTime"/> value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="columnName"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// No column with this name was found.<br />
    /// -- or --<br />
    /// The column has different type then <see cref="DateTime"/>.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
    /// -- or --<br />
    /// The underlying stream is non-seekable and columns are read out of order.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
    public virtual DateTime GetDate(string columnName)
    {
      return GetValue<DateTime>(columnName);
    }

    /// <summary>
    /// Gets a <see cref="DateTime"/> value of the specified column of the current row.
    /// </summary>
    /// <param name="column">The column.</param>
    /// <returns>A <see cref="DateTime"/> value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="column"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The column has different type then <see cref="DateTime"/>.<br />
    /// -- or --<br />
    /// The column is from different table instance.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
    /// -- or --<br />
    /// The underlying stream is non-seekable and columns are read out of order.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
    public virtual DateTime GetDate(IColumn column)
    {
      CheckColumn(column);

      return GetValue<DateTime>(column);
    }

    /// <summary>
    /// Gets a <see cref="Boolean"/> value of the specified column of the current row.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    /// <returns>A <see cref="Boolean"/> value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="columnName"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// No column with this name was found.<br />
    /// -- or --<br />
    /// The column has different type then <see cref="Boolean"/>.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
    /// -- or --<br />
    /// The underlying stream is non-seekable and columns are read out of order.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
    public virtual bool GetBoolean(string columnName)
    {
      return GetValue<bool>(columnName);
    }


    /// <summary>
    /// Gets a <see cref="Boolean"/> value of the specified column of the current row.
    /// </summary>
    /// <param name="column">The column.</param>
    /// <returns>A <see cref="Boolean"/> value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="column"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The column has different type then <see cref="Boolean"/>.<br />
    /// -- or --<br />
    /// The column is from different table instance.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
    /// -- or --<br />
    /// The underlying stream is non-seekable and columns are read out of order.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
    public virtual bool GetBoolean(IColumn column)
    {
      CheckColumn(column);

      return GetValue<bool>(column);
    }

    /// <summary>
    /// Gets a <see cref="Int32"/> value of the specified column of the current row.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    /// <returns>A <see cref="Int32"/> value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="columnName"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// No column with this name was found.<br />
    /// -- or --<br />
    /// The column has different type then <see cref="Int32"/>.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
    /// -- or --<br />
    /// The underlying stream is non-seekable and columns are read out of order.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
    public virtual int GetInt32(string columnName)
    {
      return GetValue<int>(columnName);
    }

    /// <summary>
    /// Gets a <see cref="Int32"/> value of the specified column of the current row.
    /// </summary>
    /// <param name="column">The column.</param>
    /// <returns>A <see cref="Int32"/> value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="column"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The column has different type then <see cref="Int32"/>.<br />
    /// -- or --<br />
    /// The column is from different table instance.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
    /// -- or --<br />
    /// The underlying stream is non-seekable and columns are read out of order.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
    public virtual int GetInt32(IColumn column)
    {
      CheckColumn(column);

      return GetValue<int>(column);
    }

    /// <summary>
    /// Gets a value of the specified column of the current row.
    /// </summary>
    /// <param name="columnName">The column name.</param>
    /// <returns>A column value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="columnName"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The column is from different table instance.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
    /// -- or --<br />
    /// The underlying stream is non-seekable and columns are read out of order.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
    public virtual object GetValue(string columnName)
    {
      if (columnName == null)
      {
        throw new ArgumentNullException("columnName");
      }

      var column = (Column)FindColumnByName(columnName);

      return column.LoadValueAsObject(_buffer);
    }

    /// <summary>
    /// Gets a value of the specified column of the current row.
    /// </summary>
    /// <param name="column">The column.</param>
    /// <returns>A column value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="column"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The column is from different table instance.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
    /// -- or --<br />
    /// The underlying stream is non-seekable and columns are read out of order.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
    public virtual object GetValue(IColumn column)
    {
      CheckColumn(column);

      var columnBase = (Column)column;

      return columnBase.LoadValueAsObject(_buffer);
    }

    /// <summary>
    /// Gets a value of the specified column of the current row.
    /// </summary>
    /// <typeparam name="T">The column type.</typeparam>
    /// <param name="columnName">The column name.</param>
    /// <returns>A column value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="columnName"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The column is from different table instance.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
    /// -- or --<br />
    /// The underlying stream is non-seekable and columns are read out of order.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
    protected T GetValue<T>(string columnName)
    {
      if (columnName == null)
      {
        throw new ArgumentNullException("columnName");
      }

      var typedColumn = FindColumnByName(columnName) as Column<T>;
      if (typedColumn == null)
      {
        throw new ArgumentOutOfRangeException("columnName", "The column's type does not match the method's return type.");
      }

      return typedColumn.LoadValue(_buffer);
    }

    /// <summary>
    /// Gets a value of the specified column of the current row.
    /// </summary>
    /// <typeparam name="T">The column type.</typeparam>
    /// <param name="column">The column.</param>
    /// <returns>A column value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="column"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The column is from different table instance.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// No row is loaded. The <see cref="Read"/> method returned <c>false</c> or it has not been called yet.<br />
    /// -- or --<br />
    /// The underlying stream is non-seekable and columns are read out of order.
    /// </exception>
    /// <exception cref="ObjectDisposedException">The parent table is disposed.</exception>
    protected T GetValue<T>(IColumn column)
    {
      CheckColumn(column);

      if (column.type != typeof(T))
      {
        throw new ArgumentOutOfRangeException("column", "The column's type does not match the method's return type.");
      }

      var typedColumn = (Column<T>)column;

      return typedColumn.LoadValue(_buffer);
    }
    #endregion

    #region field update ------------------------------------------------------------------------------

    // TODO

    #endregion

    #region IsNull ------------------------------------------------------------------------------------------

    public bool IsNull(string columnName)
    {
      if (columnName == null)
      {
        throw new ArgumentNullException("columnName");
      }

      var column = (Column)FindColumnByName(columnName);

      return IsNull(column);
    }

    public bool IsNull(IColumn column)
    {
      CheckColumn(column);

      Type type = column.type;

      var typedColumn = column as Column;

      return typedColumn.IsNull(_buffer);
    }
    #endregion

    #region technical ---------------------------------------------------------------------------------------

    private void CheckColumn(IColumn column)
    {
      if (column == null)
      {
        throw new ArgumentNullException("column");
      }

      if (! _columns.Contains(column))
      {
        throw new ArgumentOutOfRangeException("column", "The column instance doesn't belong to this table.");
      }
    }

    public IColumn FindColumnByName(string columnName)
    {
      var column = _columns.FirstOrDefault(c => c.name == columnName);

      if (column == null)
      {
        throw ExceptionFactory.CreateArgumentOutOfRangeException("columnName", "Column {0} not found.", columnName);
      }

      return column;
    }
    
    #endregion

    #region GetRawString ------------------------------------------------------------------------------------

    /// <summary>
    /// Raw data of field (mostly for debug or discovery of original DBF content)
    /// </summary>
    /// <param name="columnName">Name of column</param>
    /// <returns></returns>
    public string GetRawString(string columnName)                                             // new by eMeL
    {
      if (columnName == null)
      {
        throw new ArgumentNullException("columnName");
      }

      var column = (IColumn)FindColumnByName(columnName);

      return GetRawString(column);
    }

    /// <summary>
    /// Raw data of field (mostly for debug or discovery of original DBF content)
    /// </summary>
    /// <param name="column">Column definition</param>
    /// <returns></returns>
    public string GetRawString(IColumn column)                                                 
    {
      CheckColumn(column);

      Column col = column as Column;

      string ret = Encoding.ASCII.GetString(_buffer, col.offset + 1, col.size);

      ret = ret.Replace('\0', '◌');

      return ret;
    }
    #endregion
  }
}
