using System;
using System.Text;

namespace NDbfReaderEx
{
  /// <summary>
  /// The base class of all column types. Intended for internal usage. To define a custom column type, derive from the generic subclass <see cref="Column&lt;T&gt;"/>.
  /// </summary>
  public abstract class Column : IColumn
  {
    protected readonly string           name_;
    protected readonly short            size_;
    protected readonly int              offset_;
    protected readonly Encoding         encoding_;
    protected readonly short            dec_;
    protected readonly NativeColumnType dbfType_;

    protected int                       displayWidth_;                                              // later it can be modified
    protected bool                      leftSideDisplay_;                                           // later it can be modified

    /// <summary>
    /// Initializes a new instance with the specified name, offset and size.
    /// </summary>
    /// <param name="name">The colum name.</param>
    /// <param name="offset">The column offset in a row in bytes.</param>
    /// <param name="size">The column size in bytes.</param>
    /// <param name="encoding">The encoding of column's content bytes.</param>
    /// <exception cref="ArgumentNullException"><paramref name="name"/> is <c>null</c> or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="offset"/> is &lt; 1 or <paramref name="size"/> is &lt; 1.</exception>
    protected internal Column(string name, NativeColumnType dbfType, int offset, short size, short dec, Encoding encoding)
    {
      if (string.IsNullOrEmpty(name))
      {
        throw new ArgumentNullException("name");
      }

      if (offset < 0)
      {
        throw new ArgumentOutOfRangeException("offset");
      }

      if (size < 1)
      {
        throw new ArgumentOutOfRangeException("size");
      }

      this.name_     = name;
      this.dbfType_  = dbfType;
      this.offset_   = offset;
      this.size_     = size;
      this.dec_      = dec;
      this.encoding_ = encoding;

      displayWidth_    = size;                                                          // good for a few types of colums (for example string, bool)
      leftSideDisplay_ = true;

      switch (dbfType)
      {
        case NativeColumnType.Date:
          displayWidth_ = 10;                                                           // yyyy.mm.dd
          break;

        case NativeColumnType.Memo:
          displayWidth_ = 30;                                                           // only a value, maybe good
          break;

        case NativeColumnType.Float:
        case NativeColumnType.Numeric:        
          //if (dec > 0)
          //{
          //  displayWidth_++;                                                            // for decimal dot
          //}
          leftSideDisplay_ = false;
          break;

        case NativeColumnType.Double:
          displayWidth_    = 18; 
          leftSideDisplay_ = false;
          break;

        case NativeColumnType.Long:
          displayWidth_    = 11;                                                        // -2000000000
          leftSideDisplay_ = false;
          break;
      }
    }

    /// <summary>
    /// Gets the column name.
    /// </summary>
    public string name
    {
      get
      {
        return name_;
      }
    }

    /// <summary>
    /// Gets the column offset in a row in bytes.
    /// </summary>
    public int offset
    {
      get
      {
        return offset_;
      }
    }

    /// <summary>
    /// Gets the column size in bytes.
    /// </summary>
    public short size
    {
      get
      {
        return size_;
      }
    }

    /// <summary>
    /// Gets the number of decimal places in bytes/characters.
    /// </summary>
    public short dec
    {
      get
      {
        return dec_;
      }
    }

    /// <summary>
    /// Gets the <c>DBF</c> type of a column value.
    /// </summary>
    public NativeColumnType dbfType
    {
      get
      {
        return dbfType_;
      }
    }

    /// <summary>
    /// Gets the width to display of a column value.
    /// If column type is memo, returns 0 because it is variable width.
    /// </summary>
    public int displayWidth
    {
      get
      {
        return displayWidth_;
      }
    }


    /// <summary>
    /// Better side to display of a column value.
    /// </summary>
    public bool leftSideDisplay
    {
      get
      {
        return leftSideDisplay_;
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
    public abstract object LoadValueAsObject(byte[] rowBuffer, ref byte[] cachedColumnData);

    /// <summary>
    /// Is a value from the specified rowBuffer null.
    /// </summary>
    /// <param name="rowBuffer">The byte array from which a value should be loaded.</param>
    /// <returns></returns>
    public abstract bool IsNull(byte[] rowBuffer);

    public abstract void SetNull(byte[] rowBuffer);
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
    protected Column(string name, NativeColumnType dbfType, int offset, short size, short dec, Encoding encoding)
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
    public T LoadValue(byte[] rowBuffer, ref byte[] cachedColumnData)
    {
      if (rowBuffer == null)
      {
        throw new ArgumentNullException("rowBuffer");
      }

      if (rowBuffer.Length < (offset_ + size_ - 1))
      {
        throw ExceptionFactory.CreateArgumentException("rowBuffer", "The rowBuffer must have enought bytes.");
      }

      return ValueFromRowBuffer(rowBuffer, ref cachedColumnData);
    }

    /// <summary>
    /// Loads a value from the specified rowBuffer.
    /// </summary>
    /// <param name="rowBuffer">The byte array from which a value should be loaded.</param>
    /// <returns>A column value.</returns>
    public sealed override object LoadValueAsObject(byte[] rowBuffer, ref byte[] cachedColumnData)
    {
      return LoadValue(rowBuffer, ref cachedColumnData);
    }

    /// <summary>
    /// Loads a value from the rowBuffer of row/record.
    /// </summary>
    /// <param name="rowBuffer">The byte array from which a value should be loaded. The rowBuffer length is always at least equal to the column size.</param>
    /// <returns>A column value.</returns>
    protected abstract T ValueFromRowBuffer(byte[] rowBuffer, ref byte[] cachedColumnData);

    #endregion
  }
}
