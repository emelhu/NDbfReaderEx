using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Text;

// contact:  emel@emel.hu
// If you have question or comment send to me it.
// English is not my native language, please send criticism and/or correction too.

// git push -u origin master
// https://github.com/emelhu/NDbfReader_eMeL_Fork.git
// https://github.com/emelhu/NDbfReaderEx.git

// Fileformat info:
// http://www.dbf2002.com/dbf-file-format.html                          -- DBF
// http://www.cs.cmu.edu/~varun/cs315p/xbase.txt                        -- DBF & DBT & NDX/NTX/etc.
// http://www.dbase.com/Knowledgebase/INT/db7_file_fmt.htm              -- memo DBT
// http://msdn.microsoft.com/en-us/library/aa975374(v=vs.71).aspx       -- FoxPro memo FPT
// http://ulisse.elettra.trieste.it/services/doc/dbase/DBFstruct.htm    -- DBF, a few words from DBT
// http://www.clicketyclick.dk/databases/xbase/format/index.html        -- DBT
// http://www.dbase.com/Knowledgebase/INT/db7_file_fmt.htm              -- dbase7 table

// http://tmpvar.com/markdown.html   for prewiew of *.MD files :)
// https://help.github.com/articles/github-flavored-markdown     -- help

namespace NDbfReaderEx
{
  /// <summary>
  /// Represents a dBASE table.  Use one of the Open static methods to create a new instance.
  /// </summary>
  /// <example>
  /// <code>
  /// using(var table = new DbfTable.Open(@"D:\Example\table.dbf", Encoding.GetEncoding(437)))
  /// {
  ///     ...
  /// }
  /// </code>
  /// </example>
  public partial class DbfTable : IDisposable, IEnumerable<DbfRow>
  {
    #region variables -------------------------------------------------------------------------------------

    private  readonly Stream       _stream;
    internal readonly IColumn[]    _columns;
    private  readonly Encoding     _encoding;

    private           DbfHeader    _header;
    public  readonly  Guid         dbfTableClassID = Guid.NewGuid();   

    private           MemoFileBase  _memoFile;

    private           List<IndexFileBase> _indexFiles;

    private           DbfTableType _tableType  = DbfTableType.Undefined; 
    
    private           bool         _disposed   = false;

    public            bool         skipDeleted = true;                             // leave out deleted rows from result (Enumerate)
    public            bool         memoStreamCloseWhenDispose = true;              // detached rows can or can't read it after DbfTable closed.

    #endregion

    #region Constructor ---------------------------------------------------------------------------------

    protected DbfTable(Stream stream, Encoding encoding = null)
    {
      this._stream = stream;
    
      RefreshHeaderInfo();

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

      this._columns  = ReadDbfColumns(_stream, this.encoding);                            // this.encoding: property, because return default is null

      //

      int calcLen = _header.firstRecordPosition + (_header.recCount * _header.rowLength) + 1;

      if ((stream.Length < calcLen) || (stream.Length > calcLen + 1))  
      { // dBase & Clipper different (There is or there isn't a 0x1F character at end of DBF data file .
        throw ExceptionFactory.CreateArgumentOutOfRangeException("DBF table", "Datafile length error! [got: {0} expected: {1}]", stream.Length, calcLen);
      }      
    }

    static DbfTable()
    {
      Initialize_CodepageCodes_Encoding();
    }
    
    #endregion

    #region Dbf file control  -------------------------------------------------------------------------------

    public void RefreshHeaderInfo()
    {
      _header = ReadDbfHeader(_stream);

      return;
    }

    #endregion

    #region Open/Create DBF table --------------------------------------------------------------------------

    public static DbfTable Open(string path, DbfTableOpenMode openMode, Encoding encoding = null, DbfTableType tableType = DbfTableType.Undefined)
    {
      if (string.IsNullOrEmpty(path))
      {
        throw new ArgumentNullException("path");
      }

      FileAccess access;
      FileShare  share;

      switch (openMode)
      {
        case DbfTableOpenMode.Exclusive:
          access = FileAccess.ReadWrite;
          share  = FileShare.None;
          break;
        case DbfTableOpenMode.ReadWrite:
          access = FileAccess.ReadWrite;
          share  = FileShare.ReadWrite;
          break;
        case DbfTableOpenMode.Read:
          access = FileAccess.Read;
          share  = FileShare.ReadWrite;
          break;
        default:
          throw ExceptionFactory.CreateArgumentException("DbfTable/Open(Openmode)", "Invalid open mode parameter!");
      }

      var dbfFile = Open(new FileStream(path, FileMode.Open, access, share), encoding);

      dbfFile.fileNameDBF = path;
      dbfFile.tableType   = tableType;
      dbfFile.openMode    = openMode;

      if (dbfFile.isExistsMemoField)
      { // If exist a memo field and it is opened from a file, so I can find DBT/FPT/etc. memeo file too.
        dbfFile.JoinMemoFile();
      }

      return dbfFile;
    }

    /// <summary>
    /// Opens a table from the specified file.
    /// </summary>
    /// <param name="path">The file to be opened.</param>
    /// <returns>A table instance.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="path"/> is <c>null</c> or empty.</exception>
    /// <exception cref="NotSupportedException">The dBASE table constains one or more columns of unsupported type.</exception>
    public static DbfTable Open(string path, Encoding encoding = null, DbfTableType tableType = DbfTableType.Undefined)
    {
      if (string.IsNullOrEmpty(path))
      {
        throw new ArgumentNullException("path");
      }

      var dbfFile = Open(new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite), encoding);

      dbfFile.fileNameDBF = path;
      dbfFile.tableType   = tableType;
      dbfFile.openMode    = DbfTableOpenMode.Read;

      if (dbfFile.isExistsMemoField)
      { // If exist a memo field and it is opened from a file, so I can find DBT/FPT/etc. memeo file too.
        dbfFile.JoinMemoFile();
      }

      return dbfFile;
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

    //

    /// <summary>
    /// Create a new DBF datafile.
    /// Create() can't overwrite exists file - it's a precautionary measure.
    /// </summary>
    /// <param name="path">A not exists filename.</param>
    /// <param name="columns">Definition of columns</param>
    /// <param name="encoding">Encoding for open created file. -- It mean too, CodepageCodes of new file will OEM</param>
    /// <returns></returns>
    public static DbfTable Create(string path, IEnumerable<ColumnDefinitionForCreateTable> columns, Encoding encoding, DbfTableType tableType = DbfTableType.Undefined)
    {
      if (string.IsNullOrEmpty(path))
      {
        throw new ArgumentNullException("path");
      }

      if (columns == null)
      {
        throw new ArgumentNullException("columns");
      }

      //

      var streamDBF = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);

      DbfTable dbfTable = CreateHeader_DBF(streamDBF, columns, tableType, CodepageCodes.OEM, encoding);

      if (dbfTable.isExistsMemoField)
      {
        Stream streamMemo = CreateHeader_Memo(path, tableType);
        dbfTable.JoinMemoStream(streamMemo, tableType);
      }

      return dbfTable;  
    }

    /// <summary>
    /// Create a new DBF datafile.
    /// Create() can't overwrite exists file - it's a precautionary measure.
    /// </summary>
    /// <param name="path">A not exists filename.</param>
    /// <param name="columns">Definition of columns</param>
    /// <param name="codepageCode">Stored encoding information code, allways auto create appropriate encoding for open this file.</param>
    /// <returns></returns>
    public static DbfTable Create(string path, IEnumerable<ColumnDefinitionForCreateTable> columns, CodepageCodes codepageCode, DbfTableType tableType = DbfTableType.Undefined)
    {
      if (string.IsNullOrEmpty(path))
      {
        throw new ArgumentNullException("path");
      }

      if (columns == null)
      {
        throw new ArgumentNullException("columns");
      }

      //

      var stream = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None);

      return CreateHeader_DBF(stream, columns, tableType, codepageCode);                    
    }
    #endregion

    #region Interface/Read dbf info  -----------------------------------------------------------------------
    
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
    public int          recCount  { get { return _header.recCount; } }

    public DbfHeader    dbfHeader { get { return _header; } }

    public DbfTableType tableType 
    { 
      get 
      { 
        if (_tableType == DbfTableType.Undefined)
        {
          return defaultTableType;
        }

        return _tableType; 
      } 
      
      internal set 
      {
        _tableType = value;
      } 
    }


    public Encoding encoding 
    { 
      get 
      { 
        if (_encoding == null)
        {
          return defaultEncoding;
        }

        return _encoding; 
      } 
    }

    //

    public bool isExistsMemoField
    {
      get
      {
        return Array.Exists(_columns, (c => c.dbfType == NativeColumnType.Memo));
      }
    }

    public bool isEnabledMemoField
    {
      get
      {
        return (isExistsMemoField && (_memoFile != null));      
      }
    }

    //

    public DbfTableOpenMode openMode     { get; internal set; }                       // Don't forget an info, if available...  
    public string           fileNameDBF  { get; internal set; }                       // Don't forget an info, if available... good for open memo stream automatically
    public string           fileNameMemo { get; internal set; }                       // Don't forget an info, if available...
    
    //

    public static Encoding     defaultEncoding  = null; 
    public static DbfTableType defaultTableType = DbfTableType.Undefined;

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

      if (memoStreamCloseWhenDispose && (_memoFile != null))
      {
        _memoFile.Dispose();
      }

      //

      if (_indexFiles != null)
      {
        foreach (var indexFile in _indexFiles)
        {
          indexFile.Dispose();
        }
      }
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

    #region Dbf reader/enumerator -------------------------------------------------------------------------
    
    /// <summary>
    /// Compatibility behavior for original NDbfReader class of eXavera
    /// https://github.com/eXavera/NDbfReader
    /// </summary>
    public Reader OpenReader(int startRecNo = 0)
    {
      return new Reader(this, startRecNo);                                                // skipDeleted inherited from this/DbfReader
    }

    public Reader OpenReader(bool skipDeleted, int startRecNo = 0)
    {
      Reader reader = new Reader(this, startRecNo);

      reader.skipDeleted = skipDeleted;

      return reader;
    }


    public ClipperReader OpenClipperReader()
    {
      return new ClipperReader(this, this.skipDeleted);                                     
    }

    public ClipperReader OpenClipperReader(bool skipDeleted)
    {
      ClipperReader reader = new ClipperReader(this, skipDeleted);

      return reader;
    }

    //

    IEnumerator IEnumerable.GetEnumerator()
    {
      return GetEnumerator();
    }

    public IEnumerator<DbfRow> GetEnumerator()
    {
      for (int i = 0; i < this.recCount; i++)
      {
        DbfRow row = this.GetRow(i);

        if (this.skipDeleted && row.deleted)
        {
          continue;
        }

        yield return row;
      }
    }

    public IEnumerator<DbfRow> GetEnumerator(bool skipDeleted)
    {
      for (int i = 0; i < this.recCount; i++)
      {
        DbfRow row = this.GetRow(i);

        if (skipDeleted && row.deleted)
        {
          continue;
        }

        yield return row;
      }
    }
    #endregion
  }

  #region Helper struct/enum ------------------------------------------------------------------------------
  
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


  public enum DbfTableOpenMode
  { 
    Undefined = 0,
    Read,
    ReadWrite,
    Exclusive
  }


  public enum DbfTableType                                                              // format of DBF (data) and closed DBF/FPT/etc. (memo)
  {
    Undefined = 0,                                                                       
    DBase3,
    Clipper  //,
    //FoxPro
  }
  #endregion
}
