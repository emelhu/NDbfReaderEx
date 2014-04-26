using System;
using System.Text;

namespace NDbfReader
{
  /// <summary>
  /// The base class of all column types. Intended for internal usage. To define a custom column type, derive from the generic subclass <see cref="Column&lt;T&gt;"/>.
  /// </summary>
  public abstract class Column : IColumn
  {
    protected readonly string   _name;
    protected readonly short    _size;
    protected readonly int      _offset;
    protected readonly Encoding _encoding;
    protected readonly short    _dec;
    protected readonly byte     _dbfType;

    /// <summary>
    /// Initializes a new instance with the specified name, offset and size.
    /// </summary>
    /// <param name="name">The colum name.</param>
    /// <param name="offset">The column offset in a row in bytes.</param>
    /// <param name="size">The column size in bytes.</param>
    /// <param name="encoding">The encoding of column's content bytes.</param>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is &lt; 1 or <paramref name="size"/> is &lt; 1.</exception>
    protected internal Column(string name, byte dbfType, int offset, short size, short dec, Encoding encoding)
    {
      if (string.IsNullOrEmpty(name))
      {
        throw new ArgumentNullException("name");
      }

      if (offset < 1)
      {
        throw new ArgumentOutOfRangeException("offset");
      }

      if (size < 1)
      {
        throw new ArgumentOutOfRangeException("size");
      }

      this._name = name;
      this._dbfType = dbfType;
      this._offset = offset;
      this._size = size;
      this._dec = dec;
      this._encoding = encoding;
    }

    /// <summary>
    /// Gets the column name.
    /// </summary>
    public string name
    {
      get
      {
        return _name;
      }
    }

    /// <summary>
    /// Gets the column offset in a row in bytes.
    /// </summary>
    public int offset
    {
      get
      {
        return _offset;
      }
    }

    /// <summary>
    /// Gets the column size in bytes.
    /// </summary>
    public short size
    {
      get
      {
        return _size;
      }
    }

    /// <summary>
    /// Gets the number of decimal places in bytes/characters.
    /// </summary>
    public short dec
    {
      get
      {
        return _dec;
      }
    }

    /// <summary>
    /// Gets the <c>DBF</c> type of a column value.
    /// </summary>
    public byte dbfType
    {
      get
      {
        return _dbfType;
      }
    }

    /// <summary>
    /// Gets the <c>CLR</c> type of a column value.
    /// </summary>
    public abstract Type type { get; }

    /// <summary>
    /// Loads a value from the specified rowBuffer.
    /// </summary>
    /// <param name="rowBuffer">The byte array from which a value should be loaded.</param>
    /// <returns>A column value.</returns>
    public abstract object LoadValueAsObject(byte[] rowBuffer);

    /// <summary>
    /// Is a value from the specified rowBuffer null.
    /// </summary>
    /// <param name="rowBuffer">The byte array from which a value should be loaded.</param>
    /// <returns></returns>
    public abstract bool IsNull(byte[] rowBuffer);
  }

  /// <summary>
  /// The base class for all column types.
  /// </summary>
  /// <typeparam name="T">The type of the column value.</typeparam>
  public abstract class Column<T> : Column
  {
    /// <summary>
    /// Initializes a new instance with the specified name, offset and size.
    /// </summary>
    /// <param name="name">The colum name.</param>
    /// <param name="offset">The column offset in a row in bytes.</param>
    /// <param name="size">The column size in bytes.</param>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is &lt; 0 or <paramref name="size"/> is &lt; 0.</exception>
    protected Column(string name, byte dbfType, int offset, short size, short dec, Encoding encoding)
      : base(name, dbfType, offset, size, dec, encoding)
    {
    }

    /// <summary>
    /// Gets the <c>CLR</c> type of column value.
    /// </summary>
    public override Type type
    {
      get
      {
        return typeof(T);
      }
    }

    #region LoadValue ---------------------------------------------------------------------------------------
    
    /// <summary>
    /// Loads a value from the specified rowBuffer.
    /// </summary>
    /// <param name="rowBuffer">The byte array from which a value should be loaded.</param>
    /// <returns>A column value.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="rowBuffer"/> is <c>null</c> or <paramref name="encoding"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="rowBuffer"/> is smaller then the size of the column.</exception>
    public T LoadValue(byte[] rowBuffer)
    {
      if (rowBuffer == null)
      {
        throw new ArgumentNullException("rowBuffer");
      }

      if (rowBuffer.Length < (_offset + _size - 1))
      {
        throw ExceptionFactory.CreateArgumentException("rowBuffer", "The rowBuffer must have enought bytes.");
      }

      return ValueFromRowBuffer(rowBuffer);
    }

    /// <summary>
    /// Loads a value from the specified rowBuffer.
    /// </summary>
    /// <param name="rowBuffer">The byte array from which a value should be loaded.</param>
    /// <returns>A column value.</returns>
    public sealed override object LoadValueAsObject(byte[] rowBuffer)
    {
      return LoadValue(rowBuffer);
    }

    /// <summary>
    /// Loads a value from the rowBuffer of row/record.
    /// </summary>
    /// <param name="rowBuffer">The byte array from which a value should be loaded. The rowBuffer length is always at least equal to the column size.</param>
    /// <returns>A column value.</returns>
    protected abstract T ValueFromRowBuffer(byte[] rowBuffer);

    #endregion

  }
}
