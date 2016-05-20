using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace NDbfReaderEx
{
  /// <summary>
  /// Represent one row of DBF file. It is readable in detached mode too.
  /// Content of record is modifiable, but you have to write back to DbfTable, when complet.
  /// </summary>
  [Serializable]
  public class DbfRow
  {
    internal readonly IColumn[] _columns;
    internal readonly byte[]    _buffer;

    [NonSerialized()]
    internal          int       _recNo;                                                                 // internal: DbfTable.InsertRow() can modify it

    internal          bool      _modified = false;                                                      // internal: DbfTable.UpdateRow() can clear it

    [NonSerialized()]
    private           Guid      _dbfTableClassID;                                                       

    private  const    byte      DELETED_ROW_FLAG  = (byte)'*';                                          // standard value for signals deleted state
    private  const    byte      ACCEPTED_ROW_FLAG = 0x20;                                               // blank - normal/live data (undeleted)                                        

    internal const    int       forInsert_recNoValue = int.MinValue;                                    // value of recNo if this row wait for insert to a DbfTable

    /// <summary>
    /// Contructor of row.
    /// </summary>
    /// <param name="recNo">No. of row in dbf file (first is 0)</param>
    /// <param name="buffer">bytes of entire record content</param>
    /// <param name="columns">DbfTable header information for detached mode</param>
    protected internal DbfRow(int recNo, byte[] buffer, IColumn[] columns, Guid dbfTableClassID)
    {
      this._columns = columns;
      this._buffer  = buffer;
      this._recNo   = recNo;

      this._dbfTableClassID = dbfTableClassID;

      //

      var memoColumns = Array.FindAll(_columns, (c => c.dbfType == NativeColumnType.Memo));

      if (memoColumns.Length > 0)
      {
        memoCache = new MemoCache[memoColumns.Length]; 

        for (int i = 0; i < memoColumns.Length; i++)
        {
          memoCache[i] = new MemoCache();
          memoCache[i].column = memoColumns[i];                                     // Identifier of memo field
        }
      }     
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

    /// <summary>
    /// Is modified one ore more field of row include memo fields' content too
    /// </summary>
    public bool modified
    {
      get
      {
        foreach (var item in memoCache)
        {
          if (item.modified)
          {
            return true;
          }
        }

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

    public ReadOnlyCollection<IColumn> columns
    {
      get
      {
        return new ReadOnlyCollection<IColumn>(_columns);
      }
    }

    public Guid dbfTableClassID 
    { 
      get 
      {
        return _dbfTableClassID;
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
      IColumn column = FindColumnByName(columnName);
      
      return GetString(column);
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

    public virtual double GetDouble(string columnName)
    {
      return GetValue<Double>(columnName);
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

    public virtual double GetDouble(IColumn column)
    {
      CheckColumn(column);

      return GetValue<double>(column);
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

      return GetValue(column);
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

      byte[]    fieldCache = null;
      MemoCache memoItem   = null;

      if (columnBase.dbfType == NativeColumnType.Memo)
      { // for detached mode and performance: return cached memo value if available or read it and store to cache
        Debug.Assert(memoCache != null);

        memoItem = Array.Find(memoCache, (c => c.column == column));

        Debug.Assert(memoItem != null);

        fieldCache = memoItem.data;        
      }

      object ret = columnBase.LoadValueAsObject(_buffer, ref fieldCache);

      if (memoItem != null)
      {
        memoItem.data = fieldCache;
      }

      return ret;
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

      IColumn column = FindColumnByName(columnName);
      
      return GetValue<T>(column);
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
    public T GetValue<T>(IColumn column)
    {
      CheckColumn(column);

      if (! typeof(T).IsAssignableFrom(column.type))
      {
        throw new ArgumentOutOfRangeException("column", "The column's type does not match the method's return type.");
      }

      //if (column.type != typeof(T))
      //{
      //  throw new ArgumentOutOfRangeException("column", "The column's type does not match the method's return type.");
      //}

      var typedColumn = (Column<T>)column;

      byte[]    fieldCache = null;
      MemoCache memoItem   = null;

      if (typedColumn.dbfType == NativeColumnType.Memo)
      { // for detached mode and performance: return cached memo value if available or read it and store to cache
        Debug.Assert(memoCache != null);

        memoItem = Array.Find(memoCache, (c => c.column == typedColumn));

        Debug.Assert(memoItem != null);

        fieldCache = memoItem.data;        
      }

      T ret = typedColumn.LoadValue(_buffer, ref fieldCache);

      if (memoItem != null)
      {
        memoItem.data = fieldCache;
      }

      return ret;
    }
    #endregion

    #region Memo special ------------------------------------------------------------------------------------

    /// <summary>
    /// Store memos' content to cache for detached mode.
    /// </summary>
    public void CacheMemos()
    { // Only read it and this event automatically store memo stream's content to memory cache
      for (int i = 0; i < _columns.Length; i++)
      { // faster then foreach or linq 
        if (_columns[i].dbfType == NativeColumnType.Memo)
        {
          var memo = GetString(_columns[i]);
        }
      }
    }

    private class MemoCache
    {
      public IColumn column;
      public byte[]  data;                                                                                                              
      public bool    modified;
    }

    private MemoCache[] memoCache  = null;

    #endregion

    #region PoCo

    public T Get<T>() where T : class, new()                          // new from 1.3 version
    {
      T t = (T)Activator.CreateInstance(typeof(T), this);

      return t;
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
      if (String.IsNullOrWhiteSpace(columnName))
      {
        throw new ArgumentNullException("columnName");
      }

      var column = _columns.FirstOrDefault(c => (String.Compare(c.name, columnName, true) == 0));      // case insensitive

      if (column == null)
      {
        throw ExceptionFactory.CreateArgumentOutOfRangeException("columnName", "Column {0} not found.", columnName);
      }

      return column;
    }

    public IColumn FindColumnByName(string columnName, bool nullReturnEnable)
    { // There isn't default parameter value, so this function is wrote same as previous function, because this way is quicker then call it only with name parameter.
      if (String.IsNullOrWhiteSpace(columnName))
      {
        throw new ArgumentNullException("columnName");
      }

      var column = _columns.FirstOrDefault(c => (String.Compare(c.name, columnName, true) == 0));      // case insensitive

      if (column == null)
      {
        if (! nullReturnEnable)
        {
          throw ExceptionFactory.CreateArgumentOutOfRangeException("columnName", "Column {0} not found.", columnName);
        }
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

    internal void AtachedToAnotherTable(DbfTable dbfTable, int recNo)
    {
      this._dbfTableClassID = (dbfTable == null) ? Guid.Empty : dbfTable.dbfTableClassID;                                    
      this._recNo           = recNo;

      //

      if (dbfTable != null)
      {  
        if (! IsIdenticalColumnsDefinition(this._columns, dbfTable._columns))
        {
          throw ExceptionFactory.CreateArgumentException("dbfTable", "DbfRow.AtachedToAnotherTable/this.columns and dbfTable.columns aren't identical !");
        }
      }

      //

      foreach (var item in memoCache)
      {
        item.modified = ((item.data != null) && (item.data.Length > 0));

        (item.column as Column).SetNull(_buffer);
      }
    }


    public static bool IsIdenticalColumnsDefinition(ICollection<IColumn> columns1, ICollection<IColumn> columns2)
    {
      if (columns1 == null)
      {
        throw ExceptionFactory.CreateArgumentException("columns1", "Null parameter value not allowed!");
      }

      if (columns2 == null)
      {
        throw ExceptionFactory.CreateArgumentException("columns2", "Null parameter value not allowed!");
      }


      List<Column> cols1 = new List<Column>(); 
      List<Column> cols2 = new List<Column>();

      foreach (var item in columns1)
      {
        cols1.Add(item as Column);
      }

      foreach (var item in columns2)
      {
        cols2.Add(item as Column);
      }

      if (cols1.Count != cols2.Count)
      {
        return false;
      }

      cols1.Sort((col1, col2) => String.Compare(col1.name, col2.name, true));           // Ignore case
      cols2.Sort((col1, col2) => String.Compare(col1.name, col2.name, true));           // Ignore case

      for (int i = 0; (i < cols1.Count); i++)
      {
        if (String.Compare(cols1[i].name, cols2[i].name, true) != 0)                    // Ignore case 
        {
          return false;
        }

        if (cols1[i].dbfType != cols2[i].dbfType)
        {
          return false;
        }

        if (cols1[i].size != cols2[i].size)
        {
          return false;
        }

        if (cols1[i].dec != cols2[i].dec)
        {
          return false;
        }

        if (cols1[i].type != cols2[i].type)
        {
          return false;
        }

        if (cols1[i].offset != cols2[i].offset)
        {
          return false;
        }
      }

      return true;
    }
  }
}
