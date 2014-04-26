using System;
using System.Collections.ObjectModel;
using System.IO;
using System.Text;

namespace NDbfReader
{
  /// <summary>
  /// Represents a dBASE table.  Use one of the Open static methods to create a new instance.
  /// </summary>
  /// <example>
  /// <code>
  /// using(var table = new DbfTable.Open(@"D:\Example\table.dbf"))
  /// {
  ///     ...
  /// }
  /// </code>
  /// </example>
  public partial class DbfTable : IDisposable
  {
    #region variables -------------------------------------------------------------------------------------

    private readonly Stream       _stream;
    private readonly IColumn[]    _columns;
    private readonly Encoding     _encoding;

    private          DbfHeader    _header;    
    
    private          bool         _disposed = false;

    #endregion

    #region Constructor ---------------------------------------------------------------------------------

    protected DbfTable(Stream stream, Encoding encoding = null)
    {
      this._stream = stream;
    
      ResreshHeaderInfo();

      //

      if (encoding == null)
      {
        encoding = ReadDbfHeader_Encoding(_header.codepageCode);

        if (encoding == null)
        {
          throw new Exception("DbfTable: the DBF file don't contains codepage information!");
        }
      }

      this._encoding = encoding;
    }

    static DbfTable()
    {
      Initialize_CodepageCodes_Encoding();
    }
    
    #endregion

    #region Dbf file control  -------------------------------------------------------------------------------

    public void ResreshHeaderInfo()
    {
      _header = ReadDbfHeader(_stream);

      return;
    }

    #endregion

    #region Open DBF table ------------------------------------------------------------------------------

    /// <summary>
    /// Opens a table from the specified file.
    /// </summary>
    /// <param name="path">The file to be opened.</param>
    /// <returns>A table instance.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="path"/> is <c>null</c> or empty.</exception>
    /// <exception cref="NotSupportedException">The dBASE table constains one or more columns of unsupported type.</exception>
    public static DbfTable Open(string path, Encoding encoding = null)
    {
      if (string.IsNullOrEmpty(path))
      {
        throw new ArgumentNullException("path");
      }

      return Open(new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite), encoding);
    }

    /// <summary>
    /// Opens a table from the specified stream.
    /// </summary>
    /// <param name="stream">The stream of dBASE table to open. The stream is closed when the returned table instance is disposed.</param>
    /// <returns>A table instance.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="stream"/> is <c>null</c> or <paramref name="headerLoader"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="stream"/> does not allow reading.</exception>
    /// <exception cref="NotSupportedException">The dBASE table constains one or more columns of unsupported type.</exception>
    public static DbfTable Open(Stream stream, Encoding encoding = null)
    {
      if (stream == null)
      {
        throw new ArgumentNullException("stream");
      }
      
      if (! stream.CanRead)
      {
        throw ExceptionFactory.CreateArgumentException("stream", "The stream does not allow reading (CanRead property returns false).");
      }

      if (! stream.CanSeek)
      {
        throw ExceptionFactory.CreateArgumentException("stream", "The stream does not allow reading (CanSeek property returns false).");
      }
      
      return new DbfTable(stream, encoding);
    }
    #endregion

    #region Read dbf info  --------------------------------------------------------------------------------
    
    /// <summary>
    /// Gets a list of all columns in the table.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The table is disposed.</exception>
    public ReadOnlyCollection<IColumn> columns
    {
      get
      {
        ThrowIfDisposed();

        return new ReadOnlyCollection<IColumn>(_columns);
      }
    }

    /// <summary>
    /// Count of record/row of DBF table
    /// </summary>    
    public int       recCount  { get { return _header.recCount; } }

    public DbfHeader dbfHeader { get { return _header; } }
    
    #endregion

    #region Technical  ------------------------------------------------------------------------------------

    /// <summary>
    /// Closes the underlying stream.
    /// </summary>
    public void Dispose()
    {
      if (! _disposed)
      {
        Disposing();
      }
    }

    /// <summary>
    /// Releases the underlying stream.
    /// <remarks>
    /// The method is called only when the <see cref="Dispose"/> method is called for the first time.
    /// You MUST always call the base implementation.
    /// </remarks>
    /// </summary>
    protected virtual void Disposing()
    {
      _disposed = true;
      _stream.Dispose();
    }

    /// <summary>
    /// Throws a <see cref="ObjectDisposedException"/> if the table is already disposed.
    /// </summary>
    protected void ThrowIfDisposed()
    {
      if (_disposed)
      {
        throw new ObjectDisposedException(GetType().FullName);
      }
    }
    #endregion
  }

  /// <summary>
  /// Header information of DBF file (without columns definition).
  /// </summary>
  public struct DbfHeader
  {
    //#pragma warning disable 1591                                                        // warning: "Missing XML comment for publicly visible type or member"
    public DbfTable.DbfFileTypes  type;
    public DbfTable.CodepageCodes codepageCode;
    public DateTime               lastUpdate;
    public int                    recCount;
    public int                    firstRecordPosition;
    public int                    rowLength;
    //#pragma warning restore 1591
  }
}
